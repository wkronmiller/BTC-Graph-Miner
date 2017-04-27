#include<stdlib.h>
#include<stdio.h>
#include<assert.h>
#include<mpi.h>
#include<string.h>

int mpi_commsize, mpi_myrank;

typedef struct TransactionsStrings {
    size_t size;
    char * buffer;
} TransactionsStrings;

#define HASH_BITS 160
#define HASH_MAX_LENGTH HASH_BITS / 4
#define LONGS_PER_HASH (HASH_BITS / 64) + 1
#define INPUTS_PER_TRANSACTION_MAX 10
#define OUTPUTS_PER_TRANSACTION_MAX 20

// A single bitcoin address
typedef struct Address {
    unsigned long long int chunks[LONGS_PER_HASH];
} Address;

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
    //TODO
}

void seekToNewline(MPI_File * p_fh) {
    int err;
    char buf[] = {'\0'};
    MPI_Status read_status;
    while(buf[0] != '\n') {
        err = MPI_File_read(*p_fh, &buf[0], 1, MPI_CHAR, &read_status);
        handleError(err);
    }
}

// Load transaction strings for a given rank
void loadRankData(const char * source_file, const int mpi_commsize, const int mpi_myrank, TransactionsStrings * p_tstrs) {
    int err;
    // Open file
    MPI_File fh;
    err = MPI_File_open(MPI_COMM_WORLD, source_file, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);
    handleError(err);

    // Get file size
    MPI_Offset file_size;
    err = MPI_File_get_size(fh, &file_size);
    handleError(err);

    // Get current rank's starting point
    const MPI_Offset rank_chunk_size = (file_size / mpi_commsize);
    const MPI_Offset start_offset = rank_chunk_size * mpi_myrank;
    MPI_Offset end_offset = start_offset + rank_chunk_size;
    // Correct for int division floor (last rank reads to end of file)
    if(file_size - end_offset < rank_chunk_size) {
        end_offset = file_size;
    }
    // Set current rank's view
    err = MPI_File_set_view(fh, start_offset, MPI_CHAR, MPI_CHAR, "external32", MPI_INFO_NULL);
    handleError(err);
    if(mpi_myrank != 0) {
        seekToNewline(&fh);
    }

    // Get starting position
    MPI_Offset current_position;
    err = MPI_File_get_position(fh, &current_position);
    handleError(err);

    // Load safe chunks
    const size_t inner_buffer_size = end_offset - start_offset - current_position;
    char * inner_buffer = malloc(sizeof(char) * (inner_buffer_size + 1));
    inner_buffer[inner_buffer_size] = '\0';
    MPI_Status read_status;
    err = MPI_File_read(fh, inner_buffer, inner_buffer_size, MPI_CHAR, &read_status);
    handleError(err);

    // Ensure all inner data is read
    err = MPI_File_get_position(fh, &current_position);
    handleError(err);
    assert(current_position + start_offset == end_offset);

    // Load remainder until newline
    unsigned int outer_buffer_position = 0;
    char * outer_buffer = malloc(sizeof(char) * 2048 + 1);
    do {
        err = MPI_File_read(fh, &outer_buffer[outer_buffer_position], 1, MPI_CHAR, & read_status);
        handleError(err);
    } while(outer_buffer[outer_buffer_position++] != '\n');
    outer_buffer[outer_buffer_position] = '\0';

    // Combine buffers
    p_tstrs->buffer = malloc(sizeof(char) * (outer_buffer_position + inner_buffer_size) + 1); //TODO: check for off-by-ones
    unsigned int index;
    for(index = 0; index < inner_buffer_size; ++index) {
        p_tstrs->buffer[index] = inner_buffer[index];
    }
    for(index = 0; index < outer_buffer_position; ++index) {
        p_tstrs->buffer[index + inner_buffer_size] = outer_buffer[index];
    }

    // Clean up
    free(inner_buffer);
    free(outer_buffer);
    MPI_File_close(&fh);
}

unsigned int splitString(char c, char * string, char ** tokenized) {
    const unsigned int string_length = strlen(string);
    // Number of strings in tokenized
    unsigned int num_tokens = 0;
    unsigned int str_index;
    unsigned int segment_start = 0;
    char * token;
    for(str_index = 0; str_index < string_length; ++str_index) {
        if(string[str_index] == c) {
            string[str_index] = '\0';
            token = &string[segment_start];
            tokenized[num_tokens++] = token;
            segment_start = str_index + 1;
        }
    }
    if(segment_start < string_length) {
        token = &string[segment_start];
        tokenized[num_tokens++] = &string[segment_start];
    }
    return num_tokens;
}

void parseTransaction(char * transaction_line, Transaction * p_transaction) {
    char * split_transaction[2];
    if(mpi_myrank == 15) {
        fprintf(stderr, "%s\n", transaction_line);
    }
    const int num_tokens = splitString(';', transaction_line, &split_transaction[0]);
    if(num_tokens != 2) {
        fprintf(stderr, "Failed to split '%s'['%s' '%s'](%u tokens)\n", transaction_line, split_transaction[0], split_transaction[1], num_tokens);
        fprintf(stderr, "Failed rank: %u\n", mpi_myrank);
        sleep(10);
        abort();
    }
    //TODO
}

void parseRankData(TransactionsStrings * p_tstrs, Transactions * p_transactions) {
    // Split string of transactions
    char ** tokenized_lines = malloc(sizeof(char *) * p_tstrs->size);
    p_transactions->num_transactions = splitString('\n', p_tstrs->buffer, tokenized_lines);
    // Allocate space to store transaction data
    p_transactions->transactions = malloc(sizeof(Transaction) * p_transactions->num_transactions);
    // Iterate over individual transaction strings
    unsigned int line_index;
    for(line_index = 0; line_index < p_transactions->num_transactions; ++line_index) {
        parseTransaction(tokenized_lines[line_index], &(p_transactions->transactions[line_index]));
    }
    free(tokenized_lines);
}

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

    TransactionsStrings tstrs;
    loadRankData(source_file, mpi_commsize, mpi_myrank, &tstrs);

    Transactions transactions;
    parseRankData(&tstrs, &transactions);

    // Clean up
    free(transactions.transactions);
    free(tstrs.buffer);

    // Exit MPI
    MPI_Finalize();
    return EXIT_SUCCESS;
}
