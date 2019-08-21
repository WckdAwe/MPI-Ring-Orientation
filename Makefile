EXECS=directional_ring
MPICC?=mpicc

all: ${EXECS}

directional_ring: directional_ring.c
	${MPICC} -o directional_ring directional_ring.c 

clean:
	rm ${EXECS}
