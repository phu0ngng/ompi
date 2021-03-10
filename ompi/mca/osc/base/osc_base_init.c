/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2004-2017 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2014      Los Alamos National Security, LLC. All rights
 *                         reserved.
 * Copyright (c) 2016-2017 IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/util/show_help.h"
#include "ompi/constants.h"
#include "ompi/mca/mca.h"
#include "opal/mca/base/base.h"
#include "ompi/mca/osc/osc.h"
#include "ompi/mca/osc/base/base.h"
#include "ompi/info/info.h"
#include "ompi/communicator/communicator.h"
#include "ompi/win/win.h"

int
ompi_osc_base_select(ompi_win_t *win,
                     void **base,
                     size_t size,
                     int disp_unit,
                     ompi_communicator_t *comm,
                     opal_info_t *info,
                     int flavor,
                     int *model)
{
    opal_list_item_t *item;
    ompi_osc_base_component_t *best_component = NULL;
    int best_priority = -1, priority;

    if (opal_list_get_size(&ompi_osc_base_framework.framework_components) <= 0) {
        /* we don't have any components to support us... */
        return OMPI_ERR_NOT_SUPPORTED;
    }

    for (item = opal_list_get_first(&ompi_osc_base_framework.framework_components) ;
         item != opal_list_get_end(&ompi_osc_base_framework.framework_components) ;
         item = opal_list_get_next(item)) {
        ompi_osc_base_component_t *component = (ompi_osc_base_component_t*)
            ((mca_base_component_list_item_t*) item)->cli_component;

        priority = component->osc_query(win, base, size, disp_unit, comm, info, flavor);
        if (priority < 0) {
            if (MPI_WIN_FLAVOR_SHARED == flavor && OMPI_ERR_RMA_SHARED == priority) {
                /* NTH: quick fix to return OMPI_ERR_RMA_SHARED */
                return OMPI_ERR_RMA_SHARED;
            }
            continue;
        }

        if (priority > best_priority) {
            best_component = component;
            best_priority = priority;
        }
    }

    if (NULL == best_component) return OMPI_ERR_NOT_SUPPORTED;

#if OPAL_ENABLE_FT_MPI
    if(ompi_ftmpi_enabled) {
        /* check if module is tested for FT, warn if not. */
        const char* ft_whitelist="";
        opal_show_help("help-mpi-ft.txt", "module:untested:failundef", true,
            best_component->osc_version.mca_type_name,
            best_component->osc_version.mca_component_name,
            ft_whitelist);
    }
#endif /* OPAL_ENABLE_FT_MPI */
    opal_output_verbose( 10, ompi_osc_base_framework.framework_output,
                         "select: component %s selected",
                         best_component->osc_version.mca_component_name );

    return best_component->osc_select(win, base, size, disp_unit, comm, info, flavor, model);
}


int
ompi_osc_base_pick(ompi_win_t *win,
                   size_t size,
                   int disp_unit,
                   int target,
                   ompi_communicator_t *comm,
                   opal_info_t *info,
                   int flavor,
                   int *model,
                   ompi_memhandle_t *memhandle)
{
    opal_list_item_t *item;
    ompi_osc_base_component_t *best_component = NULL;

    if (opal_list_get_size(&ompi_osc_base_framework.framework_components) <= 0) {
        /* we don't have any components to support us... */
        return OMPI_ERR_NOT_SUPPORTED;
    }

    for (item = opal_list_get_first(&ompi_osc_base_framework.framework_components) ;
         item != opal_list_get_end(&ompi_osc_base_framework.framework_components) ;
         item = opal_list_get_next(item)) {
        ompi_osc_base_component_t *component = (ompi_osc_base_component_t*)
            ((mca_base_component_list_item_t*) item)->cli_component;
        if (0 == strcmp(component->osc_version.mca_component_name, memhandle->osc_component)) {
            best_component = component;
            break;
        }
    }

    if (NULL == best_component) return OMPI_ERR_NOT_SUPPORTED;

#if OPAL_ENABLE_FT_MPI
    if(ompi_ftmpi_enabled) {
        /* check if module is tested for FT, warn if not. */
        const char* ft_whitelist="";
        opal_show_help("help-mpi-ft.txt", "module:untested:failundef", true,
            best_component->osc_version.mca_type_name,
            best_component->osc_version.mca_component_name,
            ft_whitelist);
    }
#endif /* OPAL_ENABLE_FT_MPI */
    opal_output_verbose( 10, ompi_osc_base_framework.framework_output,
                         "select: component %s selected",
                         best_component->osc_version.mca_component_name );

    return best_component->osc_pick(win, size, disp_unit, target, comm, info, memhandle, model);
}


int
ompi_osc_base_get_memhandle(
                   void *base,
                   size_t size,
                   struct opal_info_t *info,
                   ompi_communicator_t *comm,
                   ompi_memhandle_t **memhandle,
                   int *memhandle_size)
{

    opal_list_item_t *item;
    ompi_osc_base_component_t *best_component = NULL;
    int best_priority = -1, priority;

    if (opal_list_get_size(&ompi_osc_base_framework.framework_components) <= 0) {
        /* we don't have any components to support us... */
        return OMPI_ERR_NOT_SUPPORTED;
    }

    for (item = opal_list_get_first(&ompi_osc_base_framework.framework_components) ;
         item != opal_list_get_end(&ompi_osc_base_framework.framework_components) ;
         item = opal_list_get_next(item)) {
        ompi_osc_base_component_t *component = (ompi_osc_base_component_t*)
            ((mca_base_component_list_item_t*) item)->cli_component;

        priority = component->osc_query(MPI_WIN_NULL, base, size, 1, comm, &ompi_mpi_info_null.info.super, MPI_WIN_FLAVOR_MEMHANDLE);
        if (priority < 0) {
            continue;
        }

        if (priority > best_priority) {
            best_component = component;
            best_priority = priority;
        }
    }

    if (NULL == best_component) return OMPI_ERR_NOT_SUPPORTED;

    best_component->osc_get_memhandle(base, size, info, comm, memhandle, memhandle_size);
}

int ompi_osc_base_release_memhandle(ompi_memhandle_t *memhandle)
{
    opal_list_item_t *item;
    ompi_osc_base_component_t *component = NULL;

    if (opal_list_get_size(&ompi_osc_base_framework.framework_components) <= 0) {
        /* we don't have any components to support us... */
        return OMPI_ERR_NOT_SUPPORTED;
    }

    for (item = opal_list_get_first(&ompi_osc_base_framework.framework_components) ;
         item != opal_list_get_end(&ompi_osc_base_framework.framework_components) ;
         item = opal_list_get_next(item)) {
        ompi_osc_base_component_t *candidate = (ompi_osc_base_component_t*)
            ((mca_base_component_list_item_t*) item)->cli_component;
        if (0 == strcmp(candidate->osc_version.mca_component_name, (memhandle)->osc_component)) {
            component = candidate;
            break;
        }
    }

    if (NULL == component) return OMPI_ERR_NOT_SUPPORTED;

#if OPAL_ENABLE_FT_MPI
    if(ompi_ftmpi_enabled) {
        /* check if module is tested for FT, warn if not. */
        const char* ft_whitelist="";
        opal_show_help("help-mpi-ft.txt", "module:untested:failundef", true,
            component->osc_version.mca_type_name,
            component->osc_version.mca_component_name,
            ft_whitelist);
    }
#endif /* OPAL_ENABLE_FT_MPI */
    opal_output_verbose( 10, ompi_osc_base_framework.framework_output,
                         "select: component %s selected",
                         component->osc_version.mca_component_name );

    return component->osc_release_memhandle(memhandle);
}
