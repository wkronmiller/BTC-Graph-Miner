all: src/main.c
	mpicc -g -I. -Wall src/main.c -o a.out
	mpicc -g -fsanitize=address -I. -Wall src/main.c -o debug.out
debug:
	mpirun -np 1 ./debug.out testInput/testInput
debug8:
	mpirun -np 8 ./debug.out testInput/testInput
test8:
	mpirun -np 8 ./a.out testInput/testInput
test8big:
	mpirun -np 16 ./a.out /mnt/BitcoinDrive/1ktransactions
grind:
	valgrind -v mpirun -np 8 ./a.out testInput/testInput
