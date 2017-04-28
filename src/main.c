#include<stdlib.h>
#include<stdio.h>
#include<assert.h>
#include<mpi.h>
#include<unistd.h>
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
#define OUTER_BUFFER_MAX 4096 //TODO: come up with rational value

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
    char outer_buffer[OUTER_BUFFER_MAX]; //TODO: find sane value
	int count = 0;
    do {
		if(outer_buffer_position == OUTER_BUFFER_MAX) {
			fprintf(stderr, "[%u] outer buffer size exceeded\n", mpi_myrank);
			sleep(1);
			abort();
		}
        err = MPI_File_read(fh, &outer_buffer[outer_buffer_position], 1, MPI_CHAR, & read_status);
        handleError(err);
		MPI_Get_count(&read_status, MPI_CHAR, &count);
		if(count == 0) {
			outer_buffer[++outer_buffer_position] = '\0';
			break;
		}
    } while(outer_buffer[outer_buffer_position++] != '\n');
    outer_buffer[outer_buffer_position] = '\0';

    // Combine buffers
    p_tstrs->buffer = malloc(sizeof(char) * (outer_buffer_position + inner_buffer_size) + 1); //TODO: check for off-by-ones
    unsigned int index;
	// Length of transaction string
	unsigned int tstr_size = 0;
    for(index = 0; index < inner_buffer_size; ++index) {
        p_tstrs->buffer[tstr_size++] = inner_buffer[index];
    }
    for(index = 0; index < outer_buffer_position; ++index) {
        p_tstrs->buffer[tstr_size++] = outer_buffer[index];
    }
	// Add null terminator
	p_tstrs->buffer[tstr_size++] = '\0';
	// Update transaction string length
	p_tstrs->size = tstr_size;

    // Clean up
    free(inner_buffer);
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
    char * split_transaction[2] = {NULL, NULL};
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
	fprintf(stderr,"[%u] has transaction string of %lu length\n", mpi_myrank, p_tstrs->size);
	sleep(1);
	MPI_Barrier(MPI_COMM_WORLD);
    char ** tokenized_lines = malloc(sizeof(char *) * p_tstrs->size);
    const unsigned int num_tokens = splitString('\n', p_tstrs->buffer, tokenized_lines);
	fprintf(stderr,"[%u] split transactions with %u tokens\n", mpi_myrank, num_tokens);
	sleep(1);
	MPI_Barrier(MPI_COMM_WORLD);
	unsigned int num_transactions = 0;
    // Allocate space to store transaction data
    p_transactions->transactions = calloc(sizeof(Transaction) * num_tokens); // p_transactions->num_transactions);
    // Iterate over individual transaction strings
    unsigned int line_index;
    for(line_index = 0; line_index < num_tokens; ++line_index) {
        // Skip empty strings
        if(strlen(tokenized_lines[line_index]) == 0) { continue; }
        parseTransaction(tokenized_lines[line_index], &(p_transactions->transactions[num_transactions++]));
    }
	p_transactions->num_transactions = num_transactions;
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
	tstrs.size = 0;
    loadRankData(source_file, mpi_commsize, mpi_myrank, &tstrs);

	fprintf(stderr, "[%u] closed file\n", mpi_myrank);
	sleep(1);
	MPI_Barrier(MPI_COMM_WORLD);
	fprintf(stderr, "[%u] parsing data\n", mpi_myrank);
	sleep(1);

    Transactions transactions;
    parseRankData(&tstrs, &transactions);

	fprintf(stderr, "[%u] parsed data\n", mpi_myrank);
	MPI_Barrier(MPI_COMM_WORLD);
	sleep(1);


    // Clean up
    free(transactions.transactions);
    free(tstrs.buffer);

    // Exit MPI
    MPI_Finalize();
    return EXIT_SUCCESS;
}
