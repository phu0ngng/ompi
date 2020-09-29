/*
 * Copyright (c) 2018-2020 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2020      Bull S.A.S. All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
/**
 * @file
 *
 * Warning: this is not for the faint of heart -- don't even bother
 * reading this source code if you don't have a strong understanding
 * of nested data structures and pointer math (remember that
 * associativity and order of C operations is *critical* in terms of
 * pointer math!).
 */

#include "ompi_config.h"

#include "mpi.h"
#include "coll_han.h"
#include "coll_han_dynamic.h"

/*
 * Routine that creates the local hierarchical sub-communicators
 * Called each time a collective is called.
 * comm: input communicator of the collective
 */
void mca_coll_han_comm_create_new(struct ompi_communicator_t *comm,
                                  mca_coll_han_module_t *han_module)
{
    int low_rank, low_size, up_rank, w_rank, w_size;
    ompi_communicator_t **low_comm = &(han_module->sub_comm[INTRA_NODE]);
    ompi_communicator_t **up_comm = &(han_module->sub_comm[INTER_NODE]);
    int vrank, *vranks;

    mca_coll_base_module_allreduce_fn_t old_allreduce;
    mca_coll_base_module_t *old_allreduce_module;
    mca_coll_base_module_allgather_fn_t old_allgather;
    mca_coll_base_module_t *old_allgather_module;

    mca_coll_base_module_bcast_fn_t old_bcast;
    mca_coll_base_module_t *old_bcast_module;

    mca_coll_base_module_gather_fn_t old_gather;
    mca_coll_base_module_t *old_gather_module;

    mca_coll_base_module_reduce_fn_t old_reduce;
    mca_coll_base_module_t *old_reduce_module;

    /* The sub communicators have already been created */
    if (NULL != han_module->sub_comm[INTRA_NODE]
        && NULL != han_module->sub_comm[INTER_NODE]
        && NULL != han_module->cached_vranks) {
        return;
    }

    /*
     * We cannot use han allreduce and allgather without sub-communicators
     * Temporary set previous ones
     *
     * Allgather is used to compute vranks
     * Allreduce is used by ompi_comm_split_type in create_intranode_comm_new
     * Reduce + Bcast may be called by the allreduce implementation
     * Gather + Bcast may be called by the allgather implementation
     */
    old_allreduce = comm->c_coll->coll_allreduce;
    old_allreduce_module = comm->c_coll->coll_allreduce_module;

    old_allgather = comm->c_coll->coll_allgather;
    old_allgather_module = comm->c_coll->coll_allgather_module;

    old_reduce = comm->c_coll->coll_reduce;
    old_reduce_module = comm->c_coll->coll_reduce_module;

    old_bcast = comm->c_coll->coll_bcast;
    old_bcast_module = comm->c_coll->coll_bcast_module;

    old_gather = comm->c_coll->coll_gather;
    old_gather_module = comm->c_coll->coll_gather_module;

    comm->c_coll->coll_allreduce = han_module->previous_allreduce;
    comm->c_coll->coll_allreduce_module = han_module->previous_allreduce_module;

    comm->c_coll->coll_allgather = han_module->previous_allgather;
    comm->c_coll->coll_allgather_module = han_module->previous_allgather_module;

    comm->c_coll->coll_reduce = han_module->previous_reduce;
    comm->c_coll->coll_reduce_module = han_module->previous_reduce_module;

    comm->c_coll->coll_bcast = han_module->previous_bcast;
    comm->c_coll->coll_bcast_module = han_module->previous_bcast_module;

    comm->c_coll->coll_gather = han_module->previous_gather;
    comm->c_coll->coll_gather_module = han_module->previous_gather_module;

    /* Create topological sub-communicators */
    w_rank = ompi_comm_rank(comm);
    w_size = ompi_comm_size(comm);

    opal_info_t comm_info;
    OBJ_CONSTRUCT(&comm_info, opal_info_t);
    opal_info_set(&comm_info, "ompi_comm_coll_request", "han");

    /*
     * This sub-communicator contains the ranks that share my node.
     */
    opal_info_set(&comm_info, "ompi_comm_coll_han_topo_level", "INTRA_NODE");
    ompi_comm_split_type(comm, MPI_COMM_TYPE_SHARED, 0,
                         &comm_info, low_comm);

    /*
     * Get my local rank and the local size
     */
    low_size = ompi_comm_size(*low_comm);
    low_rank = ompi_comm_rank(*low_comm);

    /*
     * This sub-communicator contains one process per node: processes with the
     * same intra-node rank id share such a sub-communicator
     */
    opal_info_set(&comm_info, "ompi_comm_coll_han_topo_level", "INTER_NODE");
    ompi_comm_split_with_info(comm, w_rank, low_rank,
                              &comm_info, up_comm, false);

    up_rank = ompi_comm_rank(*up_comm);

    /*
     * Set my virtual rank number.
     * my rank # = <intra-node comm size> * <inter-node rank number>
     *             + <intra-node rank number>
     * WARNING: this formula works only if the ranks are perfectly spread over
     *          the nodes
     * TODO: find a better way of doing
     */
    vrank = low_size * up_rank + low_rank;
    vranks = (int *)malloc(sizeof(int) * w_size);
    /*
     * gather vrank from each process so every process will know other processes
     * vrank
     */
    comm->c_coll->coll_allgather(&vrank,
                                 1,
                                 MPI_INT,
                                 vranks,
                                 1,
                                 MPI_INT,
                                 comm,
                                 comm->c_coll->coll_allgather_module);

    /*
     * Set the cached info
     */
    han_module->cached_vranks = vranks;

    /* Put allreduce, allgather, reduce, bcast and gather back */
    comm->c_coll->coll_allreduce = old_allreduce;
    comm->c_coll->coll_allreduce_module = old_allreduce_module;

    comm->c_coll->coll_allgather = old_allgather;
    comm->c_coll->coll_allgather_module = old_allgather_module;

    comm->c_coll->coll_reduce = old_reduce;
    comm->c_coll->coll_reduce_module = old_reduce_module;

    comm->c_coll->coll_bcast = old_bcast;
    comm->c_coll->coll_bcast_module = old_bcast_module;

    comm->c_coll->coll_gather = old_gather;
    comm->c_coll->coll_gather_module = old_gather_module;

    OBJ_DESTRUCT(&comm_info);
}


/*
 * Routine that creates the local hierarchical sub-communicators
 * Called each time a collective is called.
 * comm: input communicator of the collective
 */
void mca_coll_han_comm_create(struct ompi_communicator_t *comm,
                              mca_coll_han_module_t *han_module)
{
    int low_rank, low_size, up_rank, w_rank, w_size;
    ompi_communicator_t **low_comms;
    ompi_communicator_t **up_comms;
    int vrank, *vranks;

    mca_coll_base_module_allreduce_fn_t old_allreduce;
    mca_coll_base_module_t *old_allreduce_module;

    mca_coll_base_module_allgather_fn_t old_allgather;
    mca_coll_base_module_t *old_allgather_module;

    mca_coll_base_module_bcast_fn_t old_bcast;
    mca_coll_base_module_t *old_bcast_module;

    mca_coll_base_module_gather_fn_t old_gather;
    mca_coll_base_module_t *old_gather_module;

    mca_coll_base_module_reduce_fn_t old_reduce;
    mca_coll_base_module_t *old_reduce_module;

    /* use cached communicators if possible */
    if (han_module->cached_comm == comm &&
        han_module->cached_low_comms != NULL &&
        han_module->cached_up_comms != NULL &&
        han_module->cached_vranks != NULL) {
        return;
    }

    /*
     * We cannot use han allreduce and allgather without sub-communicators
     * Temporary set previous ones
     *
     * Allgather is used to compute vranks
     * Allreduce is used by ompi_comm_split_type in create_intranode_comm_new
     * Reduce + Bcast may be called by the allreduce implementation
     * Gather + Bcast may be called by the allgather implementation
     */
    old_allreduce = comm->c_coll->coll_allreduce;
    old_allreduce_module = comm->c_coll->coll_allreduce_module;

    old_allgather = comm->c_coll->coll_allgather;
    old_allgather_module = comm->c_coll->coll_allgather_module;

    old_reduce = comm->c_coll->coll_reduce;
    old_reduce_module = comm->c_coll->coll_reduce_module;

    old_bcast = comm->c_coll->coll_bcast;
    old_bcast_module = comm->c_coll->coll_bcast_module;

    old_gather = comm->c_coll->coll_gather;
    old_gather_module = comm->c_coll->coll_gather_module;

    comm->c_coll->coll_allreduce = han_module->previous_allreduce;
    comm->c_coll->coll_allreduce_module = han_module->previous_allreduce_module;

    comm->c_coll->coll_allgather = han_module->previous_allgather;
    comm->c_coll->coll_allgather_module = han_module->previous_allgather_module;

    comm->c_coll->coll_reduce = han_module->previous_reduce;
    comm->c_coll->coll_reduce_module = han_module->previous_reduce_module;

    comm->c_coll->coll_bcast = han_module->previous_bcast;
    comm->c_coll->coll_bcast_module = han_module->previous_bcast_module;

    comm->c_coll->coll_gather = han_module->previous_gather;
    comm->c_coll->coll_gather_module = han_module->previous_gather_module;


    /* create communicators if there is no cached communicator */

    w_rank = ompi_comm_rank(comm);
    w_size = ompi_comm_size(comm);
    low_comms = (struct ompi_communicator_t **)malloc(COLL_HAN_LOW_MODULES *
                                                      sizeof(struct ompi_communicator_t *));
    up_comms = (struct ompi_communicator_t **)malloc(COLL_HAN_UP_MODULES *
                                                     sizeof(struct ompi_communicator_t *));

    opal_info_t comm_info;
    OBJ_CONSTRUCT(&comm_info, opal_info_t);
    opal_info_set(&comm_info, "ompi_comm_coll_ignore", "han");

    /*
     * Create the intranode sub-communicator and request sm
     */
    opal_info_set(&comm_info, "ompi_comm_coll_request", "sm");
    ompi_comm_split_type(comm, MPI_COMM_TYPE_SHARED, 0,
                         &comm_info, &(low_comms[0]));

    /*
     * Create the intranode sub-communicator and request shared
     */
    opal_info_set(&comm_info, "ompi_comm_coll_request", "shared");
    ompi_comm_split_type(comm, MPI_COMM_TYPE_SHARED, 0,
                         &comm_info, &(low_comms[1]));

    /*
     * Get my local rank and the local size
     */
    low_size = ompi_comm_size(low_comms[0]);
    low_rank = ompi_comm_rank(low_comms[0]);

    /*
     * Create the internode sub-communicator and request libnbc
     * This sub-communicator contains one process per node: processes with the
     * same intra-node rank id share such a sub-communicator
     */
    opal_info_set(&comm_info, "ompi_comm_coll_request", "libnbc");
    ompi_comm_split_with_info(comm, w_rank, low_rank,
                              &comm_info, &(up_comms[0]), false);


    up_rank = ompi_comm_rank(up_comms[0]);

    /*
     * Create the internode sub-communicator and request adapt
     * This sub-communicator contains one process per node.
     */
    opal_info_set(&comm_info, "ompi_comm_coll_request", "adapt");
    ompi_comm_split_with_info(comm, w_rank, low_rank,
                              &comm_info, &(up_comms[1]), false);

    /*
     * Set my virtual rank number.
     * my rank # = <intra-node comm size> * <inter-node rank number>
     *             + <intra-node rank number>
     * WARNING: this formula works only if the ranks are perfectly spread over
     *          the nodes
     * TODO: find a better way of doing
     */
    vrank = low_size * up_rank + low_rank;
    vranks = (int *)malloc(sizeof(int) * w_size);
    /*
     * gather vrank from each process so every process will know other processes
     * vrank
     */
    comm->c_coll->coll_allgather(&vrank, 1, MPI_INT, vranks, 1, MPI_INT, comm,
                                 comm->c_coll->coll_allgather_module);

    /*
     * Set the cached info
     */
    han_module->cached_comm = comm;
    han_module->cached_low_comms = low_comms;
    han_module->cached_up_comms = up_comms;
    han_module->cached_vranks = vranks;

    /* Put allreduce, allgather, reduce, bcast and gather back */
    comm->c_coll->coll_allreduce = old_allreduce;
    comm->c_coll->coll_allreduce_module = old_allreduce_module;

    comm->c_coll->coll_allgather = old_allgather;
    comm->c_coll->coll_allgather_module = old_allgather_module;

    comm->c_coll->coll_reduce = old_reduce;
    comm->c_coll->coll_reduce_module = old_reduce_module;

    comm->c_coll->coll_bcast = old_bcast;
    comm->c_coll->coll_bcast_module = old_bcast_module;

    comm->c_coll->coll_gather = old_gather;
    comm->c_coll->coll_gather_module = old_gather_module;

    OBJ_DESTRUCT(&comm_info);
}


