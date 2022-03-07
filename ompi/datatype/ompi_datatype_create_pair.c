/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2006 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2022 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2006 High Performance Computing Center Stuttgart,
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2006 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2009      Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2017      Research Organization for Information Science
 *                         and Technology (RIST). All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#include <stddef.h>

#include "ompi/datatype/ompi_datatype.h"
#include "ompi/datatype/ompi_datatype_internal.h"
#include "opal/util/minmax.h"
#include "opal/util/string_copy.h"
#include "opal/util/printf.h"

static ompi_datatype_t* pair_cache[OMPI_DATATYPE_MPI_MAX_PREDEFINED][OMPI_DATATYPE_MPI_MAX_PREDEFINED] = {{NULL}};

int32_t ompi_datatype_create_pair(ompi_datatype_t* valueType, ompi_datatype_t* indexType,
                                  ompi_datatype_t** newType)
{
    int rc;
    ompi_datatype_t *pdt;

    /* check the cached pair types and create a new object if needed */
    if (NULL == (pdt = pair_cache[valueType->id][indexType->id])) {

        /* complex types not supported */
        if (0 != (valueType->super.flags & OMPI_DATATYPE_FLAG_DATA_COMPLEX) ||
            0 != (indexType->super.flags & OMPI_DATATYPE_FLAG_DATA_COMPLEX)) {
            return OMPI_ERR_BAD_PARAM;
        }

        /* Mixing Fortran and C types not supported */
        if (!(((valueType->super.flags & OMPI_DATATYPE_FLAG_DATA_C) &
               (valueType->super.flags & OMPI_DATATYPE_FLAG_DATA_C)) ||
              ((indexType->super.flags & OMPI_DATATYPE_FLAG_DATA_FORTRAN) &
               (indexType->super.flags & OMPI_DATATYPE_FLAG_DATA_FORTRAN)))) {
            return OMPI_ERR_TYPE_MISMATCH;
        }

        int blocklens[2] = { 1, 1 };
        ptrdiff_t displs[2];
        ompi_datatype_t* dtypes[2] = {valueType, indexType};

        /* C or Fortran flag */
        int flags = (indexType->super.flags & OMPI_DATATYPE_FLAG_DATA_C)
                  | (indexType->super.flags & OMPI_DATATYPE_FLAG_DATA_FORTRAN);

        /* the first element is always at offset 0 in C */
        displs[0] = 0;

        /* the offset of the second element is determined by the size of the first element
        * and the alignment requirement of the second element */
        displs[1] = opal_max(valueType->super.size, indexType->super.align);

        /* create the structure */
        rc = ompi_datatype_create_struct(2, blocklens, displs, dtypes, &pdt);
        if (OMPI_SUCCESS != rc) {
            return rc;
        }

        /* commit the type */
        rc = ompi_datatype_commit(&pdt);
        if (OMPI_SUCCESS != rc) {
            OBJ_RELEASE(pdt);
            return rc;
        }

        /* Set the flags for a predefined type */
        pdt->super.flags &= ~OPAL_DATATYPE_FLAG_PREDEFINED;
        pdt->super.flags |= OMPI_DATATYPE_FLAG_PREDEFINED |
                            OMPI_DATATYPE_FLAG_ANALYZED   |
                            OMPI_DATATYPE_FLAG_MONOTONIC  |
                            flags;

        pdt->id = OMPI_DATATYPE_MPI_2LOC_GENERIC;

        /* Generate the name */
        char name[MPI_MAX_OBJECT_NAME];
        opal_snprintf("Pair[%s,%s]", MPI_MAX_OBJECT_NAME, valueType->name, indexType->name);
        opal_string_copy( pdt->name, name, MPI_MAX_OBJECT_NAME );

        /* put the type in the cache */
        pair_cache[valueType->id][indexType->id] = pdt;
    }

    *newType = pdt;
    return OMPI_SUCCESS;
}
