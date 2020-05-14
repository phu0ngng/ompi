/*
 * Copyright (c) 2018-2020 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "coll_han_trigger.h"

static void mca_coll_task_constructor(mca_coll_task_t * t)
{
    t->func_ptr = NULL;
    t->func_args = NULL;
}

static void mca_coll_task_destructor(mca_coll_task_t * t)
{
    t->func_ptr = NULL;
    t->func_args = NULL;
}

OBJ_CLASS_INSTANCE(mca_coll_task_t, opal_object_t, mca_coll_task_constructor,
                   mca_coll_task_destructor);

/* Init task */
int init_task(mca_coll_task_t * t, task_func_ptr func_ptr, void *func_args)
{
    OBJ_CONSTRUCT(t, mca_coll_task_t);
    t->func_ptr = func_ptr;
    t->func_args = func_args;
    return OMPI_SUCCESS;
}

/* Issue the task */
int issue_task(mca_coll_task_t * t)
{
    return t->func_ptr(t->func_args);
}
