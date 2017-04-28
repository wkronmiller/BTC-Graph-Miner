#include<stdlib.h>
#include<stdio.h>
#include<assert.h>
#include<mpi.h>
#include<unistd.h>
#include<string.h>
#include<inttypes.h>

int mpi_commsize, mpi_myrank;

typedef struct TransactionsStrings {
    size_t size;
    char * buffer;
} TransactionsStrings;

#define HASH_BITS 160
//#define HASH_MAX_LENGTH HASH_BITS / 4
#define HASH_MAX_LENGTH 50 //TODO: figure out why hex strings are longer than they are supposed to be
#define LONGS_PER_HASH (HASH_BITS / 64) + 1
#define INPUTS_PER_TRANSACTION_MAX 10
#define OUTPUTS_PER_TRANSACTION_MAX 20
#define OUTER_BUFFER_MAX 4096 //TODO: come up with rational value

#define HEX_TRUNCATE_START_INDEX 4
#define HEX_TRUNCATE_END_INDEX HEX_TRUNCATE_START_INDEX + (sizeof(Address) * 2)
#define TRUNCATED_LENGTH HEX_TRUNCATE_END_INDEX - HEX_TRUNCATE_START_INDEX

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
    MPI_Offset inner_end_offset = start_offset + rank_chunk_size;
    // Correct for int division floor (last rank reads to end of file)
    if(file_size - inner_end_offset < rank_chunk_size) {
        inner_end_offset = file_size;
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
    const size_t inner_buffer_size = inner_end_offset - start_offset - current_position;
    char * inner_buffer = malloc(sizeof(char) * (inner_buffer_size + 1));
    inner_buffer[inner_buffer_size] = '\0';
    MPI_Status read_status;
    err = MPI_File_read(fh, inner_buffer, inner_buffer_size, MPI_CHAR, &read_status);
    handleError(err);

    // Ensure all inner data is read
    err = MPI_File_get_position(fh, &current_position);
    handleError(err);
    assert(current_position + start_offset == inner_end_offset);

    // Load remainder until newline
    size_t outer_buffer_sane_max = file_size - inner_end_offset; //TODO: double-check this makes sense (this should mean outer buffer cannot read past end of file)
    unsigned int outer_buffer_position = 0;
    char outer_buffer[OUTER_BUFFER_MAX] = {0};
	int count = 0;
    do {
        if(outer_buffer_position == OUTER_BUFFER_MAX) {
			fprintf(stderr, "[%u] outer buffer size exceeded\n", mpi_myrank);
			sleep(1);
			abort();
		}
        if(outer_buffer_position == outer_buffer_sane_max) {
            break;
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

// Create truncated bitcoin address

void hashHexToAddress(char * hexHash, Address * p_address) {
    char truncated_address[TRUNCATED_LENGTH + 1] = {0};
    strncpy(truncated_address, &hexHash[HEX_TRUNCATE_START_INDEX], TRUNCATED_LENGTH);
    // Hex to longlong
    *p_address = strtoull(truncated_address, NULL, 16);
}

unsigned int parseAddresses(const unsigned int max_addrs, char * address_list, Address ** p_addresses) {
    // Split address list into individual strings
    char * address_strings[max_addrs];
    const unsigned int num_tokens = splitString(',', address_list, &address_strings[0]);
    // Allocate address array
    (*p_addresses) = malloc(sizeof(Address) * num_tokens);
    unsigned int token_index;
    unsigned int num_addresses = 0;
    for(token_index = 0; token_index < num_tokens; ++token_index) {
        // Skip empty strings
        if(strlen(address_strings[token_index]) > 0) {
            hashHexToAddress(address_strings[token_index], &(*p_addresses)[num_addresses++]);
        }
        assert(num_addresses <= num_tokens);
    }
    return num_addresses;
}

void parseTransaction(char * transaction_line, Transaction * p_transaction) {
    // Separate inputs and outputs
    char * split_transaction[2] = {NULL, NULL};
    const int num_tokens = splitString(';', transaction_line, &split_transaction[0]);
    if(num_tokens != 2) {
        fprintf(stderr, "Failed to split '%s'['%s' '%s'](%u tokens)\n", transaction_line, split_transaction[0], split_transaction[1], num_tokens);
        fprintf(stderr, "Failed rank: %u\n", mpi_myrank);
        sleep(10);
        abort();
    }
    p_transaction->num_inputs = parseAddresses(INPUTS_PER_TRANSACTION_MAX, split_transaction[0], &(p_transaction->inputs));
    p_transaction->num_outputs = parseAddresses(OUTPUTS_PER_TRANSACTION_MAX, split_transaction[1], &(p_transaction->outputs));
}

void parseRankData(TransactionsStrings * p_tstrs, Transactions * p_transactions) {
    char ** tokenized_lines = malloc(sizeof(char *) * p_tstrs->size);
    const unsigned int num_tokens = splitString('\n', p_tstrs->buffer, tokenized_lines);
	unsigned int num_transactions = 0;
    // Allocate space to store transaction data
    p_transactions->transactions = malloc(sizeof(Transaction) * num_tokens); // p_transactions->num_transactions);
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
