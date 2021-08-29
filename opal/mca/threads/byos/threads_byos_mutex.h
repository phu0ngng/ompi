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

#ifndef OPAL_MCA_THREADS_BYOS_THREADS_BYOS_MUTEX_H
#define OPAL_MCA_THREADS_BYOS_THREADS_BYOS_MUTEX_H

/**
 * @file:
 *
 * Mutual exclusion functions: Unix implementation.
 *
 * Functions for locking of critical sections.
 *
 * On unix, use pthreads or our own atomic operations as
 * available.
 */

#include "opal_config.h"

#include <errno.h>
#include <pthread.h>
#include <stdio.h>

#include "opal/class/opal_object.h"
#include "opal/constants.h"
#include "opal/util/output.h"
#include "opal/sys/atomic.h"

#include "opal/mca/threads/byos/threads_byos.h"

BEGIN_C_DECLS

typedef struct{
    char mtx[sizeof(pthread_mutex_t)];
} opal_thread_internal_mutex_t;

typedef void (opal_mutex_init_cb_t)(void*, bool);
typedef void (opal_mutex_lock_cb_t)(void *);
typedef void (opal_mutex_unlock_cb_t)(void *);
typedef int  (opal_mutex_trylock_cb_t)(void *);
typedef void (opal_mutex_destroy_cb_t)(void *);


typedef void (opal_cond_init_cb_t)(void *);
typedef void (opal_cond_signal_cb_t)(void *);
typedef void (opal_cond_wait_cb_t)(void *, void*);
typedef void (opal_cond_bcast_cb_t)(void *);
typedef void (opal_cond_destroy_cb_t)(void *);

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
    opal_threads_byos_yield_fn_t *yield);

typedef
struct opal_threads_byos_mutex_callbacks_t {
    opal_mutex_init_cb_t *init;
    opal_mutex_lock_cb_t *lock;
    opal_mutex_unlock_cb_t *unlock;
    opal_mutex_trylock_cb_t *trylock;
    opal_mutex_destroy_cb_t *destroy;
} opal_threads_byos_mutex_callbacks_t;

#if defined(OPAL_THREAD_INTERNAL_MUTEX_REQUIRES_RUNTIME_INIT)
#error OPAL_THREAD_INTERNAL_MUTEX_REQUIRES_RUNTIME_INIT was defined where it should not be!
#endif // OPAL_THREAD_INTERNAL_MUTEX_REQUIRES_RUNTIME_INIT

#define OPAL_THREAD_INTERNAL_MUTEX_REQUIRES_RUNTIME_INIT 1

extern opal_threads_byos_mutex_callbacks_t opal_threads_byos_mutex_callbacks;


/* We don't know how to initialize the user-provided mutex, so do nothing */
#define OPAL_THREAD_INTERNAL_MUTEX_INITIALIZER {0}
#if defined(PTHREAD_RECURSIVE_MUTEX_INITIALIZER_NP)
#    define OPAL_THREAD_INTERNAL_RECURSIVE_MUTEX_INITIALIZER {0}
#elif defined(PTHREAD_RECURSIVE_MUTEX_INITIALIZER)
#    define OPAL_THREAD_INTERNAL_RECURSIVE_MUTEX_INITIALIZER {0}
#endif

static inline int opal_thread_internal_mutex_init(opal_thread_internal_mutex_t *p_mutex,
                                                  bool recursive)
{
    opal_threads_byos_mutex_callbacks.init(p_mutex, recursive);
    return OPAL_SUCCESS;
}

static inline void opal_thread_internal_mutex_lock(opal_thread_internal_mutex_t *p_mutex)
{
    opal_threads_byos_mutex_callbacks.lock(p_mutex);
}

static inline int opal_thread_internal_mutex_trylock(opal_thread_internal_mutex_t *p_mutex)
{
    return opal_threads_byos_mutex_callbacks.trylock(p_mutex);
}

static inline void opal_thread_internal_mutex_unlock(opal_thread_internal_mutex_t *p_mutex)
{
    opal_threads_byos_mutex_callbacks.unlock(p_mutex);
}

static inline void opal_thread_internal_mutex_destroy(opal_thread_internal_mutex_t *p_mutex)
{
    opal_threads_byos_mutex_callbacks.destroy(p_mutex);
}

typedef struct {
    char cond[sizeof(pthread_cond_t)];
    opal_atomic_lock_t init_lock;
    bool initialized;
} opal_thread_internal_cond_t;

typedef
struct opal_threads_byos_cond_callbacks_t {
    opal_cond_init_cb_t *init;
    opal_cond_signal_cb_t *signal;
    opal_cond_wait_cb_t *wait;
    opal_cond_bcast_cb_t *bcast;
    opal_cond_destroy_cb_t *destroy;
} opal_threads_byos_cond_callbacks_t;

extern opal_threads_byos_cond_callbacks_t opal_threads_byos_cond_callbacks;

#define OPAL_THREAD_INTERNAL_COND_INITIALIZER {.init_lock = OPAL_ATOMIC_LOCK_INIT, .initialized = false}

static inline int opal_thread_internal_cond_init(opal_thread_internal_cond_t *p_cond)
{
    opal_threads_byos_cond_callbacks.init(p_cond->cond);
    p_cond->initialized = true;
    opal_atomic_lock_init(&p_cond->init_lock, false);
    return OPAL_SUCCESS;
}

static inline void opal_thread_byos_cond_ensure_init(opal_thread_internal_cond_t *p_cond)
{
    if (OPAL_UNLIKELY(!p_cond->initialized)) {
        opal_atomic_lock(&p_cond->init_lock);
        opal_atomic_rmb();
        if (!p_cond->initialized) {
            opal_threads_byos_cond_callbacks.init(p_cond->cond);
            p_cond->initialized = true;
        }
        opal_atomic_unlock(&p_cond->init_lock);
    }
}

static inline void opal_thread_internal_cond_wait(opal_thread_internal_cond_t *p_cond,
                                                  opal_thread_internal_mutex_t *p_mutex)
{
    opal_thread_byos_cond_ensure_init(p_cond);
    opal_threads_byos_cond_callbacks.wait(p_cond->cond, p_mutex->mtx);
}

static inline void opal_thread_internal_cond_broadcast(opal_thread_internal_cond_t *p_cond)
{
    opal_thread_byos_cond_ensure_init(p_cond);
    opal_threads_byos_cond_callbacks.bcast(p_cond->cond);
}

static inline void opal_thread_internal_cond_signal(opal_thread_internal_cond_t *p_cond)
{
    opal_thread_byos_cond_ensure_init(p_cond);
    opal_threads_byos_cond_callbacks.signal(p_cond->cond);
}

static inline void opal_thread_internal_cond_destroy(opal_thread_internal_cond_t *p_cond)
{
    opal_thread_byos_cond_ensure_init(p_cond);
    opal_threads_byos_cond_callbacks.destroy(p_cond->cond);
}

END_C_DECLS

#endif /* OPAL_MCA_THREADS_BYOS_THREADS_BYOS_MUTEX_H */
