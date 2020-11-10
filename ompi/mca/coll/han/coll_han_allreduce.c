/*
 * Copyright (c) 2018-2020 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2020      Bull S.A.S. All rights reserved.
 *
 * Copyright (c) 2020      Cisco Systems, Inc.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "coll_han.h"
#include "ompi/mca/coll/base/coll_base_functions.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "coll_han_trigger.h"
#include "ompi/runtime/ompi_spc.h"

static int mca_coll_han_allreduce_t0_task(void *task_args);
static int mca_coll_han_allreduce_t1_task(void *task_args);
static int mca_coll_han_allreduce_t2_task(void *task_args);
static int mca_coll_han_allreduce_t3_task(void *task_args);

/* Only work with regular situation (each node has equal number of processes) */

static inline void
mca_coll_han_set_allreduce_args(mca_coll_han_allreduce_args_t * args,
                                mca_coll_task_t * cur_task,
                                void *sbuf,
                                void *rbuf,
                                int seg_count,
                                struct ompi_datatype_t *dtype,
                                struct ompi_op_t *op,
                                int root_up_rank,
                                int root_low_rank,
                                struct ompi_communicator_t *up_comm,
                                struct ompi_communicator_t *low_comm,
                                int num_segments,
                                int cur_seg,
                                int w_rank,
                                int last_seg_count,
                                bool noop, ompi_request_t * req, int *completed)
{
    args->cur_task = cur_task;
    args->sbuf = sbuf;
    args->rbuf = rbuf;
    args->seg_count = seg_count;
    args->dtype = dtype;
    args->op = op;
    args->root_up_rank = root_up_rank;
    args->root_low_rank = root_low_rank;
    args->up_comm = up_comm;
    args->low_comm = low_comm;
    args->num_segments = num_segments;
    args->cur_seg = cur_seg;
    args->w_rank = w_rank;
    args->last_seg_count = last_seg_count;
    args->noop = noop;
    args->req = req;
    args->completed = completed;
}

/*
 * Each segment of the messsage needs to go though 4 steps to perform MPI_Allreduce:
 *     lr: lower level (shared-memory or intra-node) reduce,
 *     ur: upper level (inter-node) reduce,
 *     ub: upper level (inter-node) bcast,
 *     lb: lower level (shared-memory or intra-node) bcast.
 * Hence, in each iteration, there is a combination of collective operations which is called a task.
 *        | seg 0 | seg 1 | seg 2 | seg 3 |
 * iter 0 |  lr   |       |       |       | task: t0, contains lr
 * iter 1 |  ur   |  lr   |       |       | task: t1, contains ur and lr
 * iter 2 |  ub   |  ur   |  lr   |       | task: t2, contains ub, ur and lr
 * iter 3 |  lb   |  ub   |  ur   |  lr   | task: t3, contains lb, ub, ur and lr
 * iter 4 |       |  lb   |  ub   |  ur   | task: t3, contains lb, ub and ur
 * iter 5 |       |       |  lb   |  ub   | task: t3, contains lb and ub
 * iter 6 |       |       |       |  lb   | task: t3, contains lb
 */

int
mca_coll_han_allreduce_intra(const void *sbuf,
                             void *rbuf,
                             int count,
                             struct ompi_datatype_t *dtype,
                             struct ompi_op_t *op,
                             struct ompi_communicator_t *comm, mca_coll_base_module_t * module)
{
    mca_coll_han_module_t *han_module = (mca_coll_han_module_t *)module;

    /* No support for non-commutative operations */
    if(!ompi_op_is_commute(op)) {
        OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                             "han cannot handle allreduce with this operation. Fall back on another component\n"));
        goto prev_allreduce_intra;
    }

    /* Create the subcommunicators */
    if( OMPI_SUCCESS != mca_coll_han_comm_create(comm, han_module) ) {
        OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                             "han cannot handle allreduce with this communicator. Drop HAN support in this communicator and fall back on another component\n"));
        /* HAN cannot work with this communicator so fallback on all collectives */
        HAN_LOAD_FALLBACK_COLLECTIVES(han_module, comm);
        return comm->c_coll->coll_allreduce(sbuf, rbuf, count, dtype, op,
                                            comm, comm->c_coll->coll_reduce_module);
    }

    ptrdiff_t extent, lb;
    size_t dtype_size;
    ompi_datatype_get_extent(dtype, &lb, &extent);
    int seg_count = count, w_rank;
    w_rank = ompi_comm_rank(comm);
    ompi_datatype_type_size(dtype, &dtype_size);

    ompi_communicator_t *low_comm;
    ompi_communicator_t *up_comm;

    /* use MCA parameters for now */
    low_comm = han_module->cached_low_comms[mca_coll_han_component.han_allreduce_low_module];
    up_comm = han_module->cached_up_comms[mca_coll_han_component.han_allreduce_up_module];
    COLL_BASE_COMPUTED_SEGCOUNT(mca_coll_han_component.han_allreduce_segsize, dtype_size,
                                seg_count);

    /* Determine number of elements sent per task. */
    OPAL_OUTPUT_VERBOSE((10, mca_coll_han_component.han_output,
                         "In HAN Allreduce seg_size %d seg_count %d count %d\n",
                         mca_coll_han_component.han_allreduce_segsize, seg_count, count));
    int num_segments = (count + seg_count - 1) / seg_count;

    int low_rank = ompi_comm_rank(low_comm);
    int root_up_rank = 0;
    int root_low_rank = 0;
    /* Create t0 task for the first segment */
    mca_coll_task_t *t0 = OBJ_NEW(mca_coll_task_t);
    /* Setup up t0 task arguments */
    int *completed = (int *) malloc(sizeof(int));
    completed[0] = 0;
    mca_coll_han_allreduce_args_t *t = malloc(sizeof(mca_coll_han_allreduce_args_t));
    mca_coll_han_set_allreduce_args(t, t0, (char *) sbuf, (char *) rbuf, seg_count, dtype, op,
                                    root_up_rank, root_low_rank, up_comm, low_comm, num_segments, 0,
                                    w_rank, count - (num_segments - 1) * seg_count,
                                    low_rank != root_low_rank, NULL, completed);
    /* Init t0 task */
    init_task(t0, mca_coll_han_allreduce_t0_task, (void *) (t));
    /* Issure t0 task */
    issue_task(t0);

    /* Create t1 tasks for the current segment */
    mca_coll_task_t *t1 = OBJ_NEW(mca_coll_task_t);
    /* Setup up t1 task arguments */
    t->cur_task = t1;
    /* Init t1 task */
    init_task(t1, mca_coll_han_allreduce_t1_task, (void *) t);
    /* Issue t1 task */
    issue_task(t1);

    /* Create t2 tasks for the current segment */
    mca_coll_task_t *t2 = OBJ_NEW(mca_coll_task_t);
    /* Setup up t2 task arguments */
    t->cur_task = t2;
    /* Init t2 task */
    init_task(t2, mca_coll_han_allreduce_t2_task, (void *) t);
    issue_task(t2);

    /* Create t3 tasks for the current segment */
    mca_coll_task_t *t3 = OBJ_NEW(mca_coll_task_t);
    /* Setup up t3 task arguments */
    t->cur_task = t3;
    /* Init t3 task */
    init_task(t3, mca_coll_han_allreduce_t3_task, (void *) t);
    issue_task(t3);

    while (t->completed[0] != t->num_segments) {
        /* Create t3 tasks for the current segment */
        mca_coll_task_t *t3 = OBJ_NEW(mca_coll_task_t);
        /* Setup up t3 task arguments */
        t->cur_task = t3;
        t->sbuf = (char *) t->sbuf + extent * t->seg_count;
        t->rbuf = (char *) t->rbuf + extent * t->seg_count;
        t->cur_seg = t->cur_seg + 1;
        /* Init t3 task */
        init_task(t3, mca_coll_han_allreduce_t3_task, (void *) t);
        issue_task(t3);
    }
    free(t->completed);
    t->completed = NULL;
    free(t);

    return OMPI_SUCCESS;

 prev_allreduce_intra:
    return han_module->previous_allreduce(sbuf, rbuf, count, dtype, op,
                                          comm, han_module->previous_allreduce_module);
}

/* t0 task */
int mca_coll_han_allreduce_t0_task(void *task_args)
{
    mca_coll_han_allreduce_args_t *t = (mca_coll_han_allreduce_args_t *) task_args;
    OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                         "[%d] HAN Allreduce:  t0 %d r_buf %d\n", t->w_rank, t->cur_seg,
                         ((int *) t->rbuf)[0]));
    OBJ_RELEASE(t->cur_task);
    ptrdiff_t extent, lb;
    ompi_datatype_get_extent(t->dtype, &lb, &extent);
    opal_timer_t start_ts = 0;
    SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTRA, &start_ts);
    if (MPI_IN_PLACE == t->sbuf) {
        if (!t->noop) {
            t->low_comm->c_coll->coll_reduce(MPI_IN_PLACE, (char *) t->rbuf, t->seg_count, t->dtype,
                                             t->op, t->root_low_rank, t->low_comm,
                                             t->low_comm->c_coll->coll_reduce_module);
        }
        else {
            t->low_comm->c_coll->coll_reduce((char *) t->rbuf, NULL, t->seg_count, t->dtype,
                                             t->op, t->root_low_rank, t->low_comm,
                                             t->low_comm->c_coll->coll_reduce_module);
        }
    }
    else {
        t->low_comm->c_coll->coll_reduce((char *) t->sbuf, (char *) t->rbuf, t->seg_count, t->dtype,
                                         t->op, t->root_low_rank, t->low_comm,
                                         t->low_comm->c_coll->coll_reduce_module);
    }
    SPC_TIMER_STOP(OMPI_SPC_HAN_ALLREDUCE_INTRA, &start_ts);
    return OMPI_SUCCESS;
}

/* t1 task */
int mca_coll_han_allreduce_t1_task(void *task_args)
{
    mca_coll_han_allreduce_args_t *t = (mca_coll_han_allreduce_args_t *) task_args;
    OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                         "[%d] HAN Allreduce:  t1 %d r_buf %d\n", t->w_rank, t->cur_seg,
                         ((int *) t->rbuf)[0]));
    OBJ_RELEASE(t->cur_task);
    ptrdiff_t extent, lb;
    ompi_datatype_get_extent(t->dtype, &lb, &extent);
    ompi_request_t *ireduce_req;
    int tmp_count = t->seg_count;
    if (!t->noop) {
        int up_rank = ompi_comm_rank(t->up_comm);
        /* ur of cur_seg */
        opal_timer_t start_ts = 0;
        SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
        if (up_rank == t->root_up_rank) {
            t->up_comm->c_coll->coll_ireduce(MPI_IN_PLACE, (char *) t->rbuf, t->seg_count, t->dtype,
                                             t->op, t->root_up_rank, t->up_comm, &ireduce_req,
                                             t->up_comm->c_coll->coll_ireduce_module);
        } else {
            t->up_comm->c_coll->coll_ireduce((char *) t->rbuf, (char *) t->rbuf, t->seg_count,
                                             t->dtype, t->op, t->root_up_rank, t->up_comm,
                                             &ireduce_req, t->up_comm->c_coll->coll_ireduce_module);
        }
        SPC_TIMER_STOP(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
    }
    /* lr of cur_seg+1 */
    if (t->cur_seg <= t->num_segments - 2) {
        if (t->cur_seg == t->num_segments - 2 && t->last_seg_count != t->seg_count) {
            tmp_count = t->last_seg_count;
        }
        opal_timer_t start_ts = 0;
        SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTRA, &start_ts);
        t->low_comm->c_coll->coll_reduce((char *) t->sbuf + extent * t->seg_count,
                                         (char *) t->rbuf + extent * t->seg_count, tmp_count,
                                         t->dtype, t->op, t->root_low_rank, t->low_comm,
                                         t->low_comm->c_coll->coll_reduce_module);
        SPC_TIMER_STOP(OMPI_SPC_HAN_ALLREDUCE_INTRA, &start_ts);
    }

    if (!t->noop) {
        opal_timer_t start_ts = 0;
        SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
        ompi_request_wait(&ireduce_req, MPI_STATUS_IGNORE);
        SPC_TIMER_STOP(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
    }

    return OMPI_SUCCESS;
}

/* t2 task */
int mca_coll_han_allreduce_t2_task(void *task_args)
{
    mca_coll_han_allreduce_args_t *t = (mca_coll_han_allreduce_args_t *) task_args;
    OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                         "[%d] HAN Allreduce:  t2 %d r_buf %d\n", t->w_rank, t->cur_seg,
                         ((int *) t->rbuf)[0]));
    OBJ_RELEASE(t->cur_task);
    ptrdiff_t extent, lb;
    ompi_datatype_get_extent(t->dtype, &lb, &extent);
    ompi_request_t *reqs[2];
    int req_count = 0;
    int tmp_count = t->seg_count;
    if (!t->noop) {
        int up_rank = ompi_comm_rank(t->up_comm);
        /* ub of cur_seg */
        opal_timer_t start_ts = 0;
        SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
        t->up_comm->c_coll->coll_ibcast((char *) t->rbuf, t->seg_count, t->dtype, t->root_up_rank,
                                        t->up_comm, &(reqs[0]),
                                        t->up_comm->c_coll->coll_ibcast_module);
        req_count++;
        /* ur of cur_seg+1 */
        if (t->cur_seg <= t->num_segments - 2) {
            if (t->cur_seg == t->num_segments - 2 && t->last_seg_count != t->seg_count) {
                tmp_count = t->last_seg_count;
            }
            if (up_rank == t->root_up_rank) {
                t->up_comm->c_coll->coll_ireduce(MPI_IN_PLACE,
                                                 (char *) t->rbuf + extent * t->seg_count,
                                                 tmp_count, t->dtype, t->op, t->root_up_rank,
                                                 t->up_comm, &(reqs[1]),
                                                 t->up_comm->c_coll->coll_ireduce_module);
            } else {
                t->up_comm->c_coll->coll_ireduce((char *) t->rbuf + extent * t->seg_count,
                                                 (char *) t->rbuf + extent * t->seg_count,
                                                 tmp_count, t->dtype, t->op, t->root_up_rank,
                                                 t->up_comm, &(reqs[1]),
                                                 t->up_comm->c_coll->coll_ireduce_module);
            }
            req_count++;
        }
        SPC_TIMER_STOP(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
    }
    /* lr of cur_seg+2 */
    if (t->cur_seg <= t->num_segments - 3) {
        if (t->cur_seg == t->num_segments - 3 && t->last_seg_count != t->seg_count) {
            tmp_count = t->last_seg_count;
        }
        opal_timer_t start_ts = 0;
        SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTRA, &start_ts);
        t->low_comm->c_coll->coll_reduce((char *) t->sbuf + 2 * extent * t->seg_count,
                                         (char *) t->rbuf + 2 * extent * t->seg_count, tmp_count,
                                         t->dtype, t->op, t->root_low_rank, t->low_comm,
                                         t->low_comm->c_coll->coll_reduce_module);
        SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTRA, &start_ts);
    }
    if (!t->noop && req_count > 0) {
        opal_timer_t start_ts = 0;
        SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
        ompi_request_wait_all(req_count, reqs, MPI_STATUSES_IGNORE);
        SPC_TIMER_STOP(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
    }


    return OMPI_SUCCESS;
}

/* t3 task */
int mca_coll_han_allreduce_t3_task(void *task_args)
{
    mca_coll_han_allreduce_args_t *t = (mca_coll_han_allreduce_args_t *) task_args;
    OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                         "[%d] HAN Allreduce:  t3 %d r_buf %d\n", t->w_rank, t->cur_seg,
                         ((int *) t->rbuf)[0]));
    OBJ_RELEASE(t->cur_task);
    ptrdiff_t extent, lb;
    ompi_datatype_get_extent(t->dtype, &lb, &extent);
    ompi_request_t *reqs[2];
    int req_count = 0;
    int tmp_count = t->seg_count;
    if (!t->noop) {
        int up_rank = ompi_comm_rank(t->up_comm);
        opal_timer_t start_ts = 0;
        SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
        /* ub of cur_seg+1 */
        if (t->cur_seg <= t->num_segments - 2) {
            if (t->cur_seg == t->num_segments - 2 && t->last_seg_count != t->seg_count) {
                tmp_count = t->last_seg_count;
            }
            t->up_comm->c_coll->coll_ibcast((char *) t->rbuf + extent * t->seg_count, t->seg_count,
                                            t->dtype, t->root_up_rank, t->up_comm, &(reqs[0]),
                                            t->up_comm->c_coll->coll_ibcast_module);
            req_count++;
        }
        /* ur of cur_seg+2 */
        if (t->cur_seg <= t->num_segments - 3) {
            if (t->cur_seg == t->num_segments - 3 && t->last_seg_count != t->seg_count) {
                tmp_count = t->last_seg_count;
            }
            if (up_rank == t->root_up_rank) {
                t->up_comm->c_coll->coll_ireduce(MPI_IN_PLACE,
                                                 (char *) t->rbuf + 2 * extent * t->seg_count,
                                                 tmp_count, t->dtype, t->op, t->root_up_rank,
                                                 t->up_comm, &(reqs[1]),
                                                 t->up_comm->c_coll->coll_ireduce_module);
            } else {
                t->up_comm->c_coll->coll_ireduce((char *) t->rbuf + 2 * extent * t->seg_count,
                                                 (char *) t->rbuf + 2 * extent * t->seg_count,
                                                 tmp_count, t->dtype, t->op, t->root_up_rank,
                                                 t->up_comm, &(reqs[1]),
                                                 t->up_comm->c_coll->coll_ireduce_module);
            }
            req_count++;
        }
        SPC_TIMER_STOP(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
    }
    /* lr of cur_seg+3 */
    opal_timer_t start_ts = 0;
    SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTRA, &start_ts);
    if (t->cur_seg <= t->num_segments - 4) {
        if (t->cur_seg == t->num_segments - 4 && t->last_seg_count != t->seg_count) {
            tmp_count = t->last_seg_count;
        }
        t->low_comm->c_coll->coll_reduce((char *) t->sbuf + 3 * extent * t->seg_count,
                                         (char *) t->rbuf + 3 * extent * t->seg_count, tmp_count,
                                         t->dtype, t->op, t->root_low_rank, t->low_comm,
                                         t->low_comm->c_coll->coll_reduce_module);
    }
    /* lb of cur_seg */
    t->low_comm->c_coll->coll_bcast((char *) t->rbuf, t->seg_count, t->dtype, t->root_low_rank,
                                    t->low_comm, t->low_comm->c_coll->coll_bcast_module);
    SPC_TIMER_STOP(OMPI_SPC_HAN_ALLREDUCE_INTRA, &start_ts);
    if (!t->noop && req_count > 0) {
        opal_timer_t start_ts = 0;
        SPC_TIMER_START(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
        ompi_request_wait_all(req_count, reqs, MPI_STATUSES_IGNORE);
        SPC_TIMER_STOP(OMPI_SPC_HAN_ALLREDUCE_INTER, &start_ts);
    }

    t->completed[0]++;
    OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                         "[%d] HAN Allreduce:  t3 %d total %d\n", t->w_rank, t->cur_seg,
                         t->completed[0]));

    return OMPI_SUCCESS;
}

static
int low_ibcast_complete_cb(ompi_request_t *request)
{
    mca_coll_han_allreduce_args_t *t = (mca_coll_han_allreduce_args_t *)request->req_complete_cb_data;

    /* signal completion */
    int complete = OPAL_THREAD_ADD_FETCH64((opal_atomic_int64_t*)t->completed, -1);

    if (0 == complete) {
        /* all operations have completed so signal completion */
        ompi_request_complete(t->req, true);
    }

    /* free the request */
    request->req_free(&request);

    free(t);

    /* returning 1 signals that we free'd the request ourselves */
    return 1;
}

static
int up_iallreduce_complete_cb(ompi_request_t *request)
{
    mca_coll_han_allreduce_args_t *t = (mca_coll_han_allreduce_args_t *)request->req_complete_cb_data;

    /* free the request */
    request->req_free(&request);

    /* on non-low-root ranks go directly to the non-blocking inra-node bcast */
    t->low_comm->c_coll->coll_ibcast((char *) t->rbuf, t->seg_count, t->dtype, t->root_low_rank,
                                     t->low_comm, &request, t->low_comm->c_coll->coll_ibcast_module);

    request->req_complete_cb_data = t;
    ompi_request_set_callback(request, &low_ibcast_complete_cb, t);

    /* returning 1 signals that we free'd the request ourselves */
    return 1;
}

int
mca_coll_han_allreduce_intra_cb(const void *sbuf,
                                void *rbuf,
                                int count,
                                struct ompi_datatype_t *dtype,
                                struct ompi_op_t *op,
                                struct ompi_communicator_t *comm, mca_coll_base_module_t * module)
{
    mca_coll_han_module_t *han_module = (mca_coll_han_module_t *)module;

    /* No support for non-commutative operations */
    if(!ompi_op_is_commute(op)) {
        OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                             "han cannot handle allreduce with this operation. Fall back on another component\n"));
        goto prev_allreduce_intra;
    }

    /* Create the subcommunicators */
    if( OMPI_SUCCESS != mca_coll_han_comm_create(comm, han_module) ) {
        OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                             "han cannot handle allreduce with this communicator. Drop HAN support in this communicator and fall back on another component\n"));
        /* HAN cannot work with this communicator so fallback on all collectives */
        HAN_LOAD_FALLBACK_COLLECTIVES(han_module, comm);
        return comm->c_coll->coll_allreduce(sbuf, rbuf, count, dtype, op,
                                            comm, comm->c_coll->coll_reduce_module);
    }

    ptrdiff_t extent, lb;
    size_t dtype_size;
    ompi_datatype_get_extent(dtype, &lb, &extent);
    int seg_count = count, w_rank;
    w_rank = ompi_comm_rank(comm);
    ompi_datatype_type_size(dtype, &dtype_size);

    ompi_communicator_t *low_comm;
    ompi_communicator_t *up_comm;

    /* use MCA parameters for now */
    low_comm = han_module->cached_low_comms[mca_coll_han_component.han_allreduce_low_module];
    up_comm = han_module->cached_up_comms[mca_coll_han_component.han_allreduce_up_module];
    COLL_BASE_COMPUTED_SEGCOUNT(mca_coll_han_component.han_allreduce_segsize, dtype_size,
                                seg_count);

    /* Determine number of elements sent per task. */
    OPAL_OUTPUT_VERBOSE((10, mca_coll_han_component.han_output,
                         "In HAN Allreduce seg_size %d seg_count %d count %d\n",
                         mca_coll_han_component.han_allreduce_segsize, seg_count, count));
    int num_segments = (count + seg_count - 1) / seg_count;
    int last_seg_count = count - (num_segments - 1) * seg_count;

    int low_rank = ompi_comm_rank(low_comm);
    int root_up_rank = 0;
    int root_low_rank = 0;
    int completed = num_segments;

    ompi_request_t *complete_req = OBJ_NEW(ompi_request_t);
    OMPI_REQUEST_INIT(complete_req, false);

    complete_req->req_state = OMPI_REQUEST_ACTIVE;
    complete_req->req_type  = OMPI_REQUEST_COLL;


    for (int cur_seg = 0; cur_seg < num_segments; ++cur_seg) {

        void *sbuf_seg = ((char*)sbuf) + extent*cur_seg;
        void *rbuf_seg = ((char*)rbuf) + extent*cur_seg;
        int tmp_count = seg_count;
        if (cur_seg == num_segments-1 && last_seg_count != seg_count) {
            tmp_count = last_seg_count;
        }
        ompi_request_t *request;

        /* Blocking intra-node reduce */
        if (MPI_IN_PLACE == sbuf) {
            if (low_rank == root_low_rank) {
                low_comm->c_coll->coll_reduce(MPI_IN_PLACE, (char *) rbuf_seg, tmp_count, dtype,
                                              op, root_low_rank, low_comm,
                                              low_comm->c_coll->coll_reduce_module);
            }
            else {
                low_comm->c_coll->coll_reduce((char *) rbuf_seg, NULL, tmp_count, dtype,
                                              op, root_low_rank, low_comm,
                                              low_comm->c_coll->coll_reduce_module);
            }
        }
        else {
            low_comm->c_coll->coll_reduce((char *) sbuf_seg, (char *) rbuf_seg, tmp_count, dtype,
                                          op, root_low_rank, low_comm,
                                          low_comm->c_coll->coll_reduce_module);
        }

        mca_coll_han_allreduce_args_t *t = malloc(sizeof(mca_coll_han_allreduce_args_t));
        mca_coll_han_set_allreduce_args(t, NULL /* ignored */, sbuf_seg, rbuf_seg, tmp_count, dtype, op,
                                        root_up_rank, root_low_rank, up_comm, low_comm, num_segments, cur_seg,
                                        w_rank, -1 /* ignored */,
                                        low_rank != root_low_rank, request, &completed);

        if (low_rank == root_low_rank) {
            /* non-blocking inter-node allreduce */
            up_comm->c_coll->coll_iallreduce(MPI_IN_PLACE, rbuf_seg,
                                             tmp_count, t->dtype, t->op,
                                             up_comm, &request,
                                             up_comm->c_coll->coll_iallreduce_module);
            request->req_complete_cb_data = t;
            ompi_request_set_callback(request, &up_iallreduce_complete_cb, t);
        } else {
            /* on non-low-root ranks go directly to the non-blocking inra-node bcast */
            low_comm->c_coll->coll_ibcast((char *) rbuf_seg, tmp_count, dtype, root_low_rank,
                                          low_comm, &request, low_comm->c_coll->coll_ibcast_module);

            request->req_complete_cb_data = t;
            ompi_request_set_callback(request, &low_ibcast_complete_cb, t);
        }


    }

    ompi_request_wait(&complete_req, MPI_STATUS_IGNORE);
    OBJ_RELEASE(complete_req);

    return OMPI_SUCCESS;

 prev_allreduce_intra:
    return han_module->previous_allreduce(sbuf, rbuf, count, dtype, op,
                                          comm, han_module->previous_allreduce_module);
}


int
mca_coll_han_allreduce_intra_simple(const void *sbuf,
                                    void *rbuf,
                                    int count,
                                    struct ompi_datatype_t *dtype,
                                    struct ompi_op_t *op,
                                    struct ompi_communicator_t *comm,
                                    mca_coll_base_module_t *module)
{
    ompi_communicator_t *low_comm;
    ompi_communicator_t *up_comm;
    int root_low_rank = 0;
    int low_rank;
    int ret;
    mca_coll_han_module_t *han_module = (mca_coll_han_module_t *)module;
#if OPAL_ENABLE_DEBUG
    mca_coll_han_component_t *cs = &mca_coll_han_component;
#endif

    OPAL_OUTPUT_VERBOSE((10, cs->han_output,
                    "[OMPI][han] in mca_coll_han_reduce_intra_simple\n"));

    // Fallback to another component if the op cannot commute
    if (! ompi_op_is_commute(op)) {
        OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                             "han cannot handle allreduce with this operation. Fall back on another component\n"));
        goto prev_allreduce;
    }

    /* Create the subcommunicators */
    if( OMPI_SUCCESS != mca_coll_han_comm_create_new(comm, han_module) ) {
        OPAL_OUTPUT_VERBOSE((30, mca_coll_han_component.han_output,
                             "han cannot handle allreduce with this communicator. Drop HAN support in this communicator and fall back on another component\n"));
        /* HAN cannot work with this communicator so fallback on all collectives */
        HAN_LOAD_FALLBACK_COLLECTIVES(han_module, comm);
        return comm->c_coll->coll_allreduce(sbuf, rbuf, count, dtype, op,
                                            comm, comm->c_coll->coll_reduce_module);
    }

    low_comm = han_module->sub_comm[INTRA_NODE];
    up_comm = han_module->sub_comm[INTER_NODE];
    low_rank = ompi_comm_rank(low_comm);

    /* Low_comm reduce */
    if (MPI_IN_PLACE == sbuf) {
        if (low_rank == root_low_rank) {
            ret = low_comm->c_coll->coll_reduce(MPI_IN_PLACE, (char *)rbuf,
                count, dtype, op, root_low_rank,
                low_comm, low_comm->c_coll->coll_reduce_module);
        }
        else {
            ret = low_comm->c_coll->coll_reduce((char *)rbuf, NULL,
                count, dtype, op, root_low_rank,
                low_comm, low_comm->c_coll->coll_reduce_module);
        }
    }
    else {
        ret = low_comm->c_coll->coll_reduce((char *)sbuf, (char *)rbuf,
                count, dtype, op, root_low_rank,
                low_comm, low_comm->c_coll->coll_reduce_module);
    }
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        OPAL_OUTPUT_VERBOSE((30, cs->han_output,
                             "HAN/ALLREDUCE: low comm reduce failed. "
                             "Falling back to another component\n"));
        goto prev_allreduce;
    }

    /* Local roots perform a allreduce on the upper comm */
    if (low_rank == root_low_rank) {
        ret = up_comm->c_coll->coll_allreduce(MPI_IN_PLACE, rbuf, count, dtype, op,
                    up_comm, up_comm->c_coll->coll_allreduce_module);
        if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
            OPAL_OUTPUT_VERBOSE((30, cs->han_output,
                             "HAN/ALLREDUCE: up comm allreduce failed. \n"));
            /*
             * Do not fallback in such a case: only root_low_ranks follow this
             * path, the other ranks are in another collective.
             * ==> Falling back would potentially lead to a hang.
             * Simply return the error
             */
            return ret;
        }
    }

    /* Low_comm bcast */
    ret = low_comm->c_coll->coll_bcast(rbuf, count, dtype,
                root_low_rank, low_comm, low_comm->c_coll->coll_bcast_module);
    if (OPAL_UNLIKELY(OMPI_SUCCESS != ret)) {
        OPAL_OUTPUT_VERBOSE((30, cs->han_output,
                             "HAN/ALLREDUCE: low comm bcast failed. "
                             "Falling back to another component\n"));
        goto prev_allreduce;
    }

    return OMPI_SUCCESS;

 prev_allreduce:
    return han_module->previous_allreduce(sbuf, rbuf, count, dtype, op,
                                          comm, han_module->previous_allreduce_module);
}

/* Find a fallback on reproducible algorithm
 * use tuned, or if impossible whatever available
 */
int
mca_coll_han_allreduce_reproducible_decision(struct ompi_communicator_t *comm,
                                             mca_coll_base_module_t *module)
{
    int w_rank = ompi_comm_rank(comm);
    mca_coll_han_module_t *han_module = (mca_coll_han_module_t *)module;

    /* populate previous modules_storage*/
    mca_coll_han_get_all_coll_modules(comm, han_module);

    /* try availability of reproducible modules*/
    int fallbacks[] = {TUNED, BASIC};
    int fallbacks_len = sizeof(fallbacks) / sizeof(*fallbacks);
    int i;
    for (i=0; i<fallbacks_len; i++) {
        int fallback = fallbacks[i];
        mca_coll_base_module_t *fallback_module
            = han_module->modules_storage.modules[fallback].module_handler;
        if (NULL != fallback_module && NULL != fallback_module->coll_allreduce) {
            if (0 == w_rank) {
                opal_output_verbose(30, mca_coll_han_component.han_output,
                                    "coll:han:allreduce_reproducible: "
                                    "fallback on %s\n",
                                    available_components[fallback].component_name);
            }
            han_module->reproducible_allreduce_module = fallback_module;
            han_module->reproducible_allreduce = fallback_module->coll_allreduce;
            return OMPI_SUCCESS;
        }
    }
    /* fallback of the fallback */
    if (0 == w_rank) {
        opal_output_verbose(5, mca_coll_han_component.han_output,
                            "coll:han:allreduce_reproducible_decision: "
                            "no reproducible fallback\n");
    }
    han_module->reproducible_allreduce_module = han_module->previous_allreduce_module;
    han_module->reproducible_allreduce = han_module->previous_allreduce;
    return OMPI_SUCCESS;
}

/* Fallback on reproducible algorithm */
int
mca_coll_han_allreduce_reproducible(const void *sbuf,
                                    void *rbuf,
                                     int count,
                                     struct ompi_datatype_t *dtype,
                                     struct ompi_op_t *op,
                                     struct ompi_communicator_t *comm,
                                     mca_coll_base_module_t *module)
{
    mca_coll_han_module_t *han_module = (mca_coll_han_module_t *)module;
    return han_module->reproducible_allreduce(sbuf, rbuf, count, dtype,
                                              op, comm,
                                              han_module
                                              ->reproducible_allreduce_module);
}
