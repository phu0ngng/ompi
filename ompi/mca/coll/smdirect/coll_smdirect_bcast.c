/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/** @file */

#include "ompi_config.h"

#include <string.h>

#include "opal/datatype/opal_convertor.h"
#include "ompi/constants.h"
#include "ompi/communicator/communicator.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mca/coll/coll.h"
#include "opal/sys/atomic.h"
#include "coll_smdirect.h"
#include "opal/datatype/opal_datatype_internal.h"


int mca_coll_smdirect_bcast_intra(void *buff, int count,
                            struct ompi_datatype_t *datatype, int root,
                            struct ompi_communicator_t *comm,
                            mca_coll_base_module_t *module)
{
    mca_coll_smdirect_module_t *sm_module = (mca_coll_smdirect_module_t*) module;
    mca_coll_smdirect_comm_t *data = sm_module->sm_comm_data;
    ptrdiff_t extent;

    /* Setup some identities */

    const int rank = ompi_comm_rank(comm);
    const int size = ompi_comm_size(comm);

    /* extent is from lb to ub (e.g., MPI_SHORT_INT is 8) */
    ompi_datatype_type_extent(datatype, &extent);
    const size_t total_extent = count * extent;
    const size_t control_size = mca_coll_smdirect_component.sm_control_size;

    /* get the current operation */
    int op_count = ++data->mcb_operation_count;

    /* wait for processes from the previous op to finish */
    FLAG_WAIT_FOR_IDLE(&data->procdata->mcsp_op_flag);

    unsigned char *dtype_store = (data->sm_bootstrap_meta->module_data_addr + control_size * root + sizeof(mca_coll_smdirect_procdata_t));
    /* set our input buffer information */
    if (root == rank) {
        /* leafs provide the full input buffer */
        data->procdata->mcsp_indata = (void*)buff;
        data->procdata->mcsp_insize = total_extent;
        /* TODO: make our datatype available */
        /* put the OPAL datatype right after our procdata */
        memcpy(dtype_store, &datatype->super, sizeof(opal_datatype_t));
        /* put the datatype description elements after it */
        dtype_store += sizeof(opal_datatype_t);
        opal_datatype_t *dtype_copy = (opal_datatype_t*)dtype_store;
        memcpy(dtype_store, datatype->super.desc.desc, datatype->super.desc.used*sizeof(dt_elem_desc_t));
        dtype_copy->desc.desc = (dt_elem_desc_t*)dtype_store;
        dtype_store += datatype->super.desc.used*sizeof(dt_elem_desc_t);
        memcpy(dtype_store, datatype->super.opt_desc.desc, datatype->super.opt_desc.used*sizeof(dt_elem_desc_t));

        /* signal that our procdata for this op is ready */
        opal_atomic_wmb();
        /* we will wait for our parent to signal completion */
        FLAG_RETAIN(&data->procdata->mcsp_op_flag, size-1, op_count);

        FLAG_WAIT_FOR_IDLE(&data->procdata->mcsp_op_flag);
    } else {

        /* get the endpoint and map the memory region */

        mca_coll_smdirect_peerdata_t *peerdata = data->peerdata;
        mca_coll_smdirect_peerdata_t *peer = &peerdata[0];

        /* get the endpoint */
        if (NULL == (peer->endpoint = data->endpoints[root])) {
            peer->endpoint = MCA_SMSC_CALL(get_endpoint, (&ompi_comm_peer_lookup(comm, root)->super));
            data->endpoints[root] = peer->endpoint;
        }

        peer->procdata = (mca_coll_smdirect_procdata_t *)(data->sm_bootstrap_meta->module_data_addr
                                                          + control_size * root);

        /* make sure we're all on the same op */
        FLAG_WAIT_FOR_OP(&peer->procdata->mcsp_op_flag, op_count);

        opal_atomic_rmb();
        /* map the children's memory region */
        peer->mapping_ctx = MCA_SMSC_CALL(map_peer_region,
                                          peer->endpoint,
                                          0,
                                          peer->procdata->mcsp_indata,
                                          peer->procdata->mcsp_insize,
                                          &peer->mapping_ptr);
        assert(peer->mapping_ptr != NULL);
        assert(peer->mapping_ctx != NULL);

        /* TODO: copy the data here */

        /* let the root know that we're done */
        opal_atomic_wmb();
        FLAG_RELEASE(&peer->procdata->mcsp_op_flag);

    }

    return OMPI_SUCCESS;
}


#if 0

/**
 * Shared memory broadcast.
 *
 * For the root, the general algorithm is to wait for a set of
 * segments to become available.  Once it is, the root claims the set
 * by writing the current operation number and the number of processes
 * using the set to the flag.  The root then loops over the set of
 * segments; for each segment, it copies a fragment of the user's
 * buffer into the shared data segment and then writes the data size
 * into its childrens' control buffers.  The process is repeated until
 * all fragments have been written.
 *
 * For non-roots, for each set of buffers, they wait until the current
 * operation number appears in the in-use flag (i.e., written by the
 * root).  Then for each segment, they wait for a nonzero to appear
 * into their control buffers.  If they have children, they copy the
 * data from their parent's shared data segment into their shared data
 * segment, and write the data size into each of their childrens'
 * control buffers.  They then copy the data from their shared [local]
 * data segment into the user's output buffer.  The process is
 * repeated until all fragments have been received.  If they do not
 * have children, they copy the data directly from the parent's shared
 * data segment into the user's output buffer.
 */
int mca_coll_smdirect_bcast_intra(void *buff, int count,
                            struct ompi_datatype_t *datatype, int root,
                            struct ompi_communicator_t *comm,
                            mca_coll_base_module_t *module)
{
    struct iovec iov;
    mca_coll_smdirect_module_t *sm_module = (mca_coll_smdirect_module_t*) module;
    mca_coll_smdirect_comm_t *data;
    int i, ret, rank, size, num_children, src_rank;
    int flag_num, segment_num, max_segment_num;
    int parent_rank;
    size_t total_size, max_data, bytes;
    mca_coll_smdirect_in_use_flag_t *flag;
    opal_convertor_t convertor;
    mca_coll_smdirect_tree_node_t *me, *parent, **children;
    mca_coll_smdirect_data_index_t *index;

    /* Lazily enable the module the first time we invoke a collective
       on it */
    if (!sm_module->enabled) {
        if (OMPI_SUCCESS != (ret = ompi_coll_smdirect_lazy_enable(module, comm))) {
            return ret;
        }
    }
    data = sm_module->sm_comm_data;

    /* Setup some identities */

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);

    OBJ_CONSTRUCT(&convertor, opal_convertor_t);
    iov.iov_len = mca_coll_smdirect_component.sm_fragment_size;
    bytes = 0;

    me = &data->mcb_tree[(rank + size - root) % size];
    parent = me->mcstn_parent;
    children = me->mcstn_children;
    num_children = me->mcstn_num_children;

    /* Only have one top-level decision as to whether I'm the root or
       not.  Do this at the slight expense of repeating a little logic
       -- but it's better than a conditional branch in every loop
       iteration. */

    /*********************************************************************
     * Root
     *********************************************************************/

    if (root == rank) {

        /* The root needs a send convertor to pack from the user's
           buffer to shared memory */

        if (OMPI_SUCCESS !=
            (ret =
             opal_convertor_copy_and_prepare_for_send(ompi_mpi_local_convertor,
                                                      &(datatype->super),
                                                      count,
                                                      buff,
                                                      0,
                                                      &convertor))) {
            return ret;
        }
        opal_convertor_get_packed_size(&convertor, &total_size);

        /* Main loop over sending fragments */

        do {
            flag_num = (data->mcb_operation_count++ %
                        mca_coll_smdirect_component.sm_comm_num_in_use_flags);

            FLAG_SETUP(flag_num, flag, data);
            FLAG_WAIT_FOR_IDLE(flag);
            FLAG_RETAIN(flag, size - 1, data->mcb_operation_count - 1);

            /* Loop over all the segments in this set */

            segment_num =
                flag_num * mca_coll_smdirect_component.sm_segs_per_inuse_flag;
            max_segment_num =
                (flag_num + 1) * mca_coll_smdirect_component.sm_segs_per_inuse_flag;
            do {
                index = &(data->mcb_data_index[segment_num]);

                /* Copy the fragment from the user buffer to my fragment
                   in the current segment */
                max_data = mca_coll_smdirect_component.sm_fragment_size;
                COPY_FRAGMENT_IN(convertor, index, rank, iov, max_data);
                bytes += max_data;

                /* Wait for the write to absolutely complete */
                opal_atomic_wmb();

                /* Tell my children that this fragment is ready */
                PARENT_NOTIFY_CHILDREN(children, num_children, index,
                                       max_data);

                ++segment_num;
            } while (bytes < total_size && segment_num < max_segment_num);
        } while (bytes < total_size);
    }

    /*********************************************************************
     * Non-root
     *********************************************************************/

    else {

        /* Non-root processes need a receive convertor to unpack from
           shared mmory to the user's buffer */

        if (OMPI_SUCCESS !=
            (ret =
             opal_convertor_copy_and_prepare_for_recv(ompi_mpi_local_convertor,
                                                      &(datatype->super),
                                                      count,
                                                      buff,
                                                      0,
                                                      &convertor))) {
            return ret;
        }
        opal_convertor_get_packed_size(&convertor, &total_size);

        /* Loop over receiving (and possibly re-sending) the
           fragments */

        do {
            flag_num = (data->mcb_operation_count %
                        mca_coll_smdirect_component.sm_comm_num_in_use_flags);

            /* Wait for the root to mark this set of segments as
               ours */
            FLAG_SETUP(flag_num, flag, data);
            FLAG_WAIT_FOR_OP(flag, data->mcb_operation_count);
            ++data->mcb_operation_count;

            /* Loop over all the segments in this set */

            segment_num =
                flag_num * mca_coll_smdirect_component.sm_segs_per_inuse_flag;
            max_segment_num =
                (flag_num + 1) * mca_coll_smdirect_component.sm_segs_per_inuse_flag;
            do {

                /* Pre-calculate some values */
                parent_rank = (parent->mcstn_id + root) % size;
                index = &(data->mcb_data_index[segment_num]);

                /* Wait for my parent to tell me that the segment is ready */
                CHILD_WAIT_FOR_NOTIFY(rank, index, max_data);

                /* If I have children, send the data to them */
                if (num_children > 0) {
                    /* Copy the fragment from the parent's portion in
                       the segment to my portion in the segment. */
                    COPY_FRAGMENT_BETWEEN(parent_rank, rank, index, max_data);

                    /* Wait for the write to absolutely complete */
                    opal_atomic_wmb();

                    /* Tell my children that this fragment is ready */
                    PARENT_NOTIFY_CHILDREN(children, num_children, index,
                                           max_data);

                    /* Set the "copy from buffer" to be my local
                       segment buffer so that we don't potentially
                       incur a non-local memory copy from the parent's
                       fan out data segment [again] when copying to
                       the user's buffer */
                    src_rank = rank;
                }

                /* If I don't have any children, set the "copy from
                   buffer" to be my parent's fan out segment to copy
                   directly from my parent */

                else {
                    src_rank = parent_rank;
                }

                /* Copy to my output buffer */
                COPY_FRAGMENT_OUT(convertor, src_rank, index, iov, max_data);

                bytes += max_data;
                ++segment_num;
            } while (bytes < total_size && segment_num < max_segment_num);

            /* Wait for all copy-out writes to complete before I say
               I'm done with the segments */
            opal_atomic_wmb();

            /* We're finished with this set of segments */
            FLAG_RELEASE(flag);
        } while (bytes < total_size);
    }

    /* Kill the convertor */

    OBJ_DESTRUCT(&convertor);

    /* All done */

    return OMPI_SUCCESS;
}
#endif // 0
