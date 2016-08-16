Author: Shilpa Murali

1. The folder mergeImpsCLicks contains the following folders:
	in
	|
	--Dimensions
	--facts
	logs
	out

2. Execute the program in Linux or Mac OS in the current working directory with the following command:
	>>./mergeImpsClicks -h (Provides "help" by suggesting the type of input to be given)
	>>./mergeImpsClicks (Takes 5 threads to run the prgram as a default value)
	>>./mergeImpsClicks -p # (Replace # with the number of worker threads that you want to create)

3.Program Implementation
	
	a. The files present in imps and clicks folders are stored in a list
	b. A dictionary is created that stores the file name as the key and a list of boolean value if the file is 	   present in imps and clicks
	c. The unique file names are stored in a queue which is used by the threads as a shared resource
	d. Threads are created as given by the user of 5 by default and the ojects are created
	e. Each thread acquires the lock to dequeue an elemnt from the queue until its not empty
	f. The thread traverses through the file to find the transaction id 
	e. If both the imps and clicks files with same date and hour have the same transcation id, the json 	   response is built using the jsonResponseBuilder and written to the ./out directory in the json format 