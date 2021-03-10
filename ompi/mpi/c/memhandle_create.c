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
 * Copyright (c) 2006      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2015      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * Copyright (c) 2017      IBM Corporation. All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "ompi_config.h"

#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/errhandler/errhandler.h"
#include "ompi/info/info.h"
#include "ompi/win/win.h"
#include "ompi/memchecker.h"

#if OMPI_BUILD_MPI_PROFILING
#if OPAL_HAVE_WEAK_SYMBOLS
#pragma weak MPIX_Memhandle_create = PMPIX_Memhandle_create
#endif
#define MPIX_Memhandle_create PMPIX_Memhandle_create
#endif

static const char FUNC_NAME[] = "MPI_Memhandle_create";


int MPIX_Memhandle_create(void *base, MPI_Aint size, MPI_Info info, MPI_Comm comm,
                         MPI_Memhandle *memhandle, int *memhandle_size)
{
    int ret = MPI_SUCCESS;

    MEMCHECKER(
        memchecker_comm(comm);
    );
    /* argument checking */
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);

        if (ompi_comm_invalid (comm)) {
            return OMPI_ERRHANDLER_NOHANDLE_INVOKE(MPI_ERR_COMM,
                                          FUNC_NAME);

        } else if (NULL == memhandle) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_WIN, FUNC_NAME);
        } else if ( size < 0 ) {
            return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_SIZE, FUNC_NAME);
        }
    }

    /* communicator must be an intracommunicator */
    if (OMPI_COMM_IS_INTER(comm)) {
        return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_COMM, FUNC_NAME);
    }

    OPAL_CR_ENTER_LIBRARY();

    /* create window and return */
    ret = ompi_memhandle_create(base, (size_t)size, info, comm, memhandle, memhandle_size);
    if (OMPI_SUCCESS != ret) {
        printf("ompi_memhandle_create failed!\n");
        *memhandle = MPI_MEMHANDLE_NULL;
        OPAL_CR_EXIT_LIBRARY();
        return OMPI_ERRHANDLER_INVOKE(comm, MPI_ERR_WIN, FUNC_NAME);
    }

    OPAL_CR_EXIT_LIBRARY();
    return MPI_SUCCESS;
}
