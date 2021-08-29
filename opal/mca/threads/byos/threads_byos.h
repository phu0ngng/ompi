/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2020      High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2021 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#ifndef OPAL_MCA_THREADS_BYOS_THREADS_BYOS_H
#define OPAL_MCA_THREADS_BYOS_THREADS_BYOS_H

#include "opal/mca/mca.h"

typedef void(opal_threads_byos_yield_fn_t)(void);

OPAL_DECLSPEC int opal_threads_byos_yield_init(const mca_base_component_t *component);

OPAL_DECLSPEC extern opal_threads_byos_yield_fn_t *opal_threads_byos_yield_fn;

#endif /* OPAL_MCA_THREADS_BYOS_THREADS_BYOS_H */
