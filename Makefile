proj2:
	mpic++ main.cpp -o proj2

test:
	@echo testing scatter-reduce...
	mpirun -np 1 proj2 sr min C
	mpirun -np 2 proj2 sr max C
	mpirun -np 4 proj2 sr avg C
	mpirun -np 5 proj2 sr number C gt 600000
	mpirun -np 5 proj2 sr number C lt 600000
	mpirun -np 20 proj2 sr min DC
	mpirun -np 3 proj2 sr min DC
	@echo
	@echo testing broadcast-gather...
	mpirun -np 1 proj2 bg min C
	mpirun -np 2 proj2 bg max C DC
	mpirun -np 4 proj2 bg avg D E I M
	mpirun -np 3 proj2 bg avg D E I M
	@echo
	@echo testing complete!

tar:
	tar -cvf EECS690_Multicore_Project02_RustyRiedel.tar main.cpp Makefile *.csv

clean:
	rm proj2
