all: src/main.c
	mpicc -g -fsanitize=address -I. -Wall src/main.c -o a.out
test8:
	mpirun -np 8 ./a.out testInput/testInput
test8big:
	mpirun -np 16 ./a.out /mnt/BitcoinDrive/1ktransactions
grind:
	valgrind -v mpirun -np 8 ./a.out testInput/testInput
