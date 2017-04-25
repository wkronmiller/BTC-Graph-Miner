all: src/main.c
	mpicc -g -I. -Wall -O3 src/main.c -o a.out
test8:
	mpirun -np 8 ./a.out testInput/testInput
