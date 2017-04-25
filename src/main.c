#include<stdlib.h>
#include<stdio.h>
#include<assert.h>
#include<mpi.h>
#include<dirent.h>

void seekToNewline(MPI_File * p_fh) {
    int err;
    char buf[] = {'\0'};
    MPI_Status read_status;
    while(buf[0] != '\n') {
        err = MPI_File_read(*p_fh, &buf[0], 1, MPI_CHAR, &read_status);
    }
}

typedef struct TransactionsStrings {
    size_t size;
    char * buffer;
} TransactionsStrings;

void loadRankData(const char * source_file, const int mpi_commsize, const int mpi_myrank, TransactionsStrings * p_tstrs) {
    int err;
    // Open file
    MPI_File fh;
    err = MPI_File_open(MPI_COMM_WORLD, source_file, MPI_MODE_RDONLY, MPI_INFO_NULL, &fh);

    // Get file size
    MPI_Offset file_size;
    err = MPI_File_get_size(fh, &file_size);

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
    if(mpi_myrank != 0) {
        seekToNewline(&fh);
    }

    // Get starting position
    MPI_Offset current_position;
    err = MPI_File_get_position(fh, &current_position);

    // Load safe chunks
    const size_t inner_buffer_size = end_offset - start_offset - current_position;
    char * inner_buffer = malloc(sizeof(char) * (inner_buffer_size + 1));
    inner_buffer[inner_buffer_size] = '\0';
    MPI_Status read_status;
    err = MPI_File_read(fh, inner_buffer, inner_buffer_size, MPI_CHAR, &read_status);

    // Ensure all inner data is read
    err = MPI_File_get_position(fh, &current_position);
    assert(current_position + start_offset == end_offset);

    // Load remainder until newline
    unsigned int outer_buffer_position = 0;
    char * outer_buffer = malloc(sizeof(char) * 2048 + 1);
    do {
        err = MPI_File_read(fh, &outer_buffer[outer_buffer_position], 1, MPI_CHAR, & read_status);
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

int main(int argc, char ** argv) {
    // Initialize MPI
    int mpi_commsize, mpi_myrank;
    MPI_Init(&argc, &argv);
    MPI_Comm_size(MPI_COMM_WORLD, &mpi_commsize);
    MPI_Comm_rank(MPI_COMM_WORLD, &mpi_myrank);

    // Load CLI Args
    char * source_file = argv[1];

    TransactionsStrings tstrs;
    loadRankData(source_file, mpi_commsize, mpi_myrank, &tstrs);

    if(mpi_myrank == 0) {
        // Debug output
        printf("%s\n", tstrs.buffer);
    }

    // Clean up
    free(tstrs.buffer);

    // Exit MPI
    MPI_Finalize();
    return EXIT_SUCCESS;
}
