/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2021 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007-2018 Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015-2016 Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2019      Sandia National Laboratories.  All rights reserved.
 * Copyright (c) 2020      Triad National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2021      Argonne National Laboratory.  All rights reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "opal/mca/threads/byos/threads_byos.h"
#include "opal/mca/threads/byos/threads_byos_mutex.h"

static void opal_thread_internal_mutex_init_pthread(void *p_mutex,
                                                    bool recursive);


opal_threads_byos_mutex_callbacks_t opal_threads_byos_mutex_callbacks = {
    .init    = &opal_thread_internal_mutex_init_pthread,
    .lock    = (opal_mutex_lock_cb_t*)&pthread_mutex_lock,
    .trylock = (opal_mutex_trylock_cb_t*)&pthread_mutex_trylock,
    .unlock  = (opal_mutex_unlock_cb_t*)&pthread_mutex_unlock,
    .destroy = (opal_mutex_destroy_cb_t*)&pthread_mutex_destroy
};

static void opal_thread_internal_cond_init_pthread(void *p_mutex);

opal_threads_byos_cond_callbacks_t opal_threads_byos_cond_callbacks = {
    .init = &opal_thread_internal_cond_init_pthread,
    .signal = (opal_cond_signal_cb_t*)&pthread_cond_signal,
    .wait = (opal_cond_wait_cb_t*)&pthread_cond_wait,
    .bcast = (opal_cond_bcast_cb_t*)&pthread_cond_broadcast,
    .destroy = (opal_cond_destroy_cb_t*)&pthread_cond_destroy
};


int opal_thread_byos_set_sync_cbs(
    /* mutex callbacks */
    opal_mutex_init_cb_t *mtx_init,
    opal_mutex_lock_cb_t *mtx_lock,
    opal_mutex_unlock_cb_t *mtx_unlock,
    opal_mutex_trylock_cb_t *mtx_trylock,
    opal_mutex_destroy_cb_t *mtx_destroy,
    /* conditional callbacks */
    opal_cond_init_cb_t *cond_init,
    opal_cond_signal_cb_t *cond_signal,
    opal_cond_wait_cb_t *cond_wait,
    opal_cond_bcast_cb_t *cond_bcast,
    opal_cond_destroy_cb_t *cond_destroy,
    /* yield callback */
    opal_threads_byos_yield_fn_t *yield)
{
    opal_threads_byos_mutex_callbacks.init = mtx_init;
    opal_threads_byos_mutex_callbacks.lock = mtx_lock;
    opal_threads_byos_mutex_callbacks.trylock = mtx_trylock;
    opal_threads_byos_mutex_callbacks.unlock = mtx_unlock;
    opal_threads_byos_mutex_callbacks.destroy = mtx_destroy;

    opal_threads_byos_cond_callbacks.init = cond_init;
    opal_threads_byos_cond_callbacks.signal = cond_signal;
    opal_threads_byos_cond_callbacks.wait = cond_wait;
    opal_threads_byos_cond_callbacks.bcast = cond_bcast;
    opal_threads_byos_cond_callbacks.destroy = cond_destroy;

    if (NULL != yield) {
        opal_threads_byos_yield_fn = yield;
    }

    return OPAL_SUCCESS;
}

void opal_thread_internal_mutex_init_pthread(void *p_mutex,
                                             bool recursive)
{
    int ret;
    pthread_mutex_t *mtx = (pthread_mutex_t*) p_mutex;
#if OPAL_ENABLE_DEBUG
    if (recursive) {
        pthread_mutexattr_t mutex_attr;
        ret = pthread_mutexattr_init(&mutex_attr);
        assert(0 == ret);
        ret = pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_RECURSIVE);
        if (0 != ret) {
            ret = pthread_mutexattr_destroy(&mutex_attr);
            assert(0 == ret);
        }
        ret = pthread_mutex_init(mtx, &mutex_attr);
        if (0 != ret) {
            ret = pthread_mutexattr_destroy(&mutex_attr);
            assert(0 == ret);
        }
        ret = pthread_mutexattr_destroy(&mutex_attr);
        assert(0 == ret);
    } else {
        ret = pthread_mutex_init(mtx, NULL);
    }
#else
    if (recursive) {
        pthread_mutexattr_t mutex_attr;
        ret = pthread_mutexattr_init(&mutex_attr);
        assert(0 == ret);
        pthread_mutexattr_settype(&mutex_attr, PTHREAD_MUTEX_RECURSIVE);
        ret = pthread_mutex_init(mtx, &mutex_attr);
        pthread_mutexattr_destroy(&mutex_attr);
    } else {
        ret = pthread_mutex_init(mtx, NULL);
    }
#endif
    assert(0 == ret);
}


static void opal_thread_internal_cond_init_pthread(void *p_cond)
{
    pthread_cond_t *cond = (pthread_cond_t*)p_cond;
    pthread_cond_init(cond, NULL);
}
