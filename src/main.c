#include<stdlib.h>
#include<stdio.h>
#include<mpi.h>

int main(int argc, char ** argv) {
    // Initialize MPI
    int mpi_commsize, mpi_myrank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_commsize);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_myrank);

    printf("[%u/%u]: Hello world\n", mpi_myrank, mpi_commsize);

    // Exit MPI
    MPI_Finalize();
    return EXIT_SUCCESS;
}
