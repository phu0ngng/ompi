/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2020      High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OMPI_REQUEST_CB_H
#define OMPI_REQUEST_CB_H

#include <assert.h>
#include "ompi_config.h"
#include "opal/class/opal_free_list.h"
#include "opal/class/opal_fifo.h"
#include "ompi/info/info.h"
#include "mpi.h"


BEGIN_C_DECLS

/**
 * Request continuation pointer states
 */
#define REQUEST_CONT_NONE (NULL)
#define REQUEST_CONT_COMPLETED (ompi_request_cont_t*)(-1ULL)

/**
 * Forward declaration
 */
typedef struct ompi_request_t ompi_request_t;

/**
 * Request callback class
 */
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_request_cont_t);

struct ompi_request_cont_t {
    opal_free_list_item_t super;      /**< Base type */
    ompi_request_t *cont_req;         /**< The continuation request this continuation is registered with */
    MPIX_Continue_cb_function *cont_cb; /**< The callback function to invoke */
    void *cont_data;                  /**< Continuation state provided by the user */
    MPI_Status *cont_status;            /**< user-provided pointers to status objects */
    int32_t num_active;   /**< The number of active operation requests on this callback */
};

/* Convenience typedef */
typedef struct ompi_request_cont_t ompi_request_cont_t;

//#define USE_FIFO 1

#ifdef USE_FIFO
extern opal_fifo_t *request_cont_fifo = NULL;
#else
extern opal_list_t *request_cont_list;
#endif

/**
 * Initialize the user-callback infrastructure.
 */
int ompi_request_cont_init(void);

/**
 * Finalize the user-callback infrastructure.
 */
int ompi_request_cont_finalize(void);


/**
 * Enqueue a completed continuation for later execution.
 */
void
ompi_request_cont_enqueue_complete(ompi_request_cont_t *cont);

/**
 * Notify the continuation that the request is complete, potentially
 * enqueueing the callback for later invocation.
 */
static inline
void ompi_request_cont_complete_req(ompi_request_cont_t *cb)
{
    int32_t num_active = OPAL_THREAD_ADD_FETCH32(&cb->num_active, -1);
    assert(num_active >= 0);
    if (0 == num_active) {
        // we were the last to deregister so enqueue for later processing
        ompi_request_cont_enqueue_complete(cb);
    }
}

/**
 * Register a request with local completion list for progressing through
 * the progress engine.
 */
int ompi_request_cont_progress_register_request(ompi_request_t *cont_req);

/**
 * Register a request with local completion list for progressing through
 * the progress engine.
 */
int ompi_request_cont_progress_deregister_request(ompi_request_t *cont_req);

/**
 * Progress a continuation request that has local completions.
 */
int ompi_request_cont_progress_request(ompi_request_t *cont_req);

/**
 * Register a continuation for a set of operations represented by \c requests.
 * If all operations have completed already the continuation will not be registered
 * and \c all_complete will be set to 1.
 */
int ompi_request_cont_register(
  ompi_request_t             *cont_req,
  int                         count,
  ompi_request_t             *requests[],
  MPIX_Continue_cb_function  *cont_cb,
  void                       *cont_data,
  bool                       *all_complete,
  ompi_status_public_t        statuses[]);


/**
 * Allocate a new (presistent & transient) continuation request.
 */
int ompi_request_cont_allocate_cont_req(ompi_request_t **cont_req, ompi_info_t *info);


/**
 * Progress outstanding ready continuations.
 */
int ompi_request_cont_progress_callback(void);


/**
 * Progress completed requests whose user-callback are pending.
 */
static inline
int ompi_request_cont_progress_ready(void)
{
    /* fast-path */
#ifdef USE_FIFO
    if (opal_fifo_is_empty(request_cont_fifo)) return OMPI_SUCCESS;
#else
    if (opal_list_is_empty(request_cont_list)) return OMPI_SUCCESS;
#endif

    ompi_request_cont_progress_callback();

    return OMPI_SUCCESS;
}

END_C_DECLS

#endif // OMPI_REQUEST_CB_H
