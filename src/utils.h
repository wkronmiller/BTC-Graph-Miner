#ifndef _RORY_UTILS
#define _RORY_UTILS 1

#include<inttypes.h>

// Globals
int mpi_commsize, mpi_myrank;

// A single truncated bitcoin address
typedef uint64_t Address;

// Transaction hash storage
typedef struct Transaction {
    unsigned int num_inputs;
    unsigned int num_outputs;
    Address * inputs;
    Address * outputs;
} Transaction;

// List of transactions
typedef struct Transactions {
    Transaction * transactions;
    unsigned int num_transactions;
} Transactions;

// Default checked function error-handler
void handleError(int err) {
    if (err) {
        char estr[256] = {0};
        int len = 0;
        MPI_Error_string(err, estr, &len);
        fprintf(stderr,"[%u]: MPI error: %s\n", mpi_myrank, estr);
		sleep(10); // Sleep to give MPI time to log message
		MPI_Abort(MPI_COMM_WORLD, err);
    }        
}

#endif
