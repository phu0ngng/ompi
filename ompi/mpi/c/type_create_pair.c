/* -*- Mode: C; c-basic-offset:4 ; indent-tabs-mode:nil -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2020 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2013      Los Alamos National Security, LLC.  All rights
 *                         reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPI_Type_create_value_index = PMPI_Type_create_value_index
#endif
#define MPI_Type_create_value_index PMPI_Type_create_value_index
#endif

static const char FUNC_NAME[] = "MPI_Type_create_pair";


int MPI_Type_create_value_index(const MPI_Datatype value_type,
                                const MPI_Datatype index_type,
                                MPI_Datatype *newtype)
{
    int rc;

    if( MPI_PARAM_CHECK ) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (MPI_DATATYPE_NULL == value_type || MPI_DATATYPE_NULL == index_type) {
            return OMPI_ERRHANDLER_NOHANDLE_INVOKE(MPI_ERR_TYPE,
                                          FUNC_NAME);
        }

        if (NULL == newtype) {
            return OMPI_ERRHANDLER_NOHANDLE_INVOKE(MPI_ERR_ARG,
                                          FUNC_NAME);
        }
    }

    rc = ompi_datatype_create_pair(value_type, index_type, newtype);
    if( rc != MPI_SUCCESS ) {
        OMPI_ERRHANDLER_NOHANDLE_RETURN( rc,
                                rc, FUNC_NAME );
    }

    return MPI_SUCCESS;
}
