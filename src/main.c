#include<stdlib.h>
#include<stdio.h>
#include<assert.h>
#include<mpi.h>
#include<unistd.h>
#include<string.h>
#include"utils.h"
#include"parser.h"

//TODO: create lookup function based on number of ranks and hash

/**
 * Can check for messages using MPI_Probe http://mpitutorial.com/tutorials/dynamic-receiving-with-mpi-probe-and-mpi-status/
 * But need to specify sender
 *
 * Could also send message to node saying not to expect any more messages
 */

int main(int argc, char ** argv) {
    // Initialize MPI
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_commsize);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_myrank);

    // Load CLI Args
    char * source_file = argv[1];

    TransactionsStrings tstrs = {0};
	tstrs.size = 0;
    loadRankData(source_file, mpi_commsize, mpi_myrank, &tstrs);

    Transactions transactions;
    parseRankData(&tstrs, &transactions);

    // Clean up
    unsigned int txn_index;
    for(txn_index = 0; txn_index < transactions.num_transactions; ++txn_index) {
        free(transactions.transactions[txn_index].inputs);
        free(transactions.transactions[txn_index].outputs);
    }
    free(transactions.transactions);
    free(tstrs.buffer);

    // Exit MPI
    MPI_Finalize();
    return EXIT_SUCCESS;
}
