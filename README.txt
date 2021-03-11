Distriduted System Programming : Assignment 2 - 2020/Spring

Students:
Moshe Dahan - 203509229
Yuval Assayg - 205785496

S3 Result bucket : https://s3.console.aws.amazon.com/s3/buckets/dsps202assignment2/?region=us-east-1&tab=overview

Instructions to run the project:
	1) Create a S3 bucket named "dsps202assignment2".
	2) Upload the following jar files to to"dsps202assignment2\jars" :
		- phaseOne.jar
		- phaseTwo.jar
		- phaseThree.jar
     	3) Upload the following txt files to to"dsps202assignment2\StopWords" :
     		- eng-stopWords.txt
     		- heb-stopWords.txt
     	4) Insert your AWS credentials into "./aws/credentials"
	5) Run Main file with AWS Credentials: java -jar startPoint.jar LANG
		when LANG can be "heb" or "eng" depends on the corpus you wish to work on. 


Job Flow:

1) startPoint: generates an EMR cluster with the mentioned steps. each step will be call Phase.
		it will also generate a unique number which is called APPID which will be passed to the Phases.
		every output and information regarding this job will be saved in :"dsps202assignment2\$APPID"


2) phaseOne:
	- Input: oneGram - part of the Google Books Ngrams. A data set containing fixed size tuples of items.
	in this case, we are extracting from n=1 corpus.
	
	- Map:	
		recieve a sequence file and creates the following Key-Value pairs:
		1. <w1, *, Decade> : - for counting w1 appearances in a given decade.
		2. <*, *, Decade> : - for counting all words appearances in a given decade (calculating N).
	
		- Note: throughout our project '*' comes before every other letter.
	      		key order in compareTo: Decade->W1->W2.
	
	
	- Combiner:
		Accumulate the number of appearances for every word per decade.
		Also, if <*,*> appears the same procedure occurs but for calculating N.
		
	- Partitioner: Partition all keys by rules set by us.
	
	- Reducer: 
		Has the same function as the combiner in case that fails.
		After accumulating it checks the type of the key and perform the following:

		1. <Decade,*,*> save the value of N for Decade in a new file under the path: "dsps202assignment2/$APPID/N"
		 2. write to context <W1, *, Decade> with the value recieved from the reducer operation as the value of Cw1.


2) phaseTwo:
	- Inputs: this phase receives multiple inputs (and different input format)- 
		1. TwoGram - part from the Google Books Ngrams. in this case we are extracting from n=2 corpus.
		2. phaseOne output - we read the Key-Value Phase One Output from the path "dsps202assignment2/$APPID/results/outputPhase1/"
	
	- Map:	creates the following Key-Value pairs:
		if receive K-V from TwoGram:
			1. <w1, w2, Decade> : - for counting w1w2 appearances in a given decade.

		if receive K-V from phaseOne output:
			1.<W1, *, Decade> with the value from the previous step
	
	
	- Combiner:
		Accumulate the number of apperances for every word or couple per decade.

		
	- Partitioner: Partition all keys by rules set by us.
	
	- Reducer:
		1. setup: read from all files in  "dsps202assignment2/$APPID/N" and build an HashMap with <Decade,N value>
		2. Has the same function as the combiner in case that fails.
		Accumulate the number of apperances for every word or couple per decade.
		
		After accumulating, it checks the type of the key and performs the following:
		<W1, *, Decade> - Since '*' comes first before every other letter we save the value of cw1.
					so we can pass it to every key from the type <W1,w2>
		
		<W1, W2, Decade>  - 
			1.get value of N for Decade from the HashMap
			2.get value of cw1 from previous operation.
			3. saves the value of cw1w2 from reducer operation
			4. creates the value of cw2 = 0 (for now)
			4.write to context <W1, W2, Decade> with the values of cw1,cw2,cw1w2,N.
		
		
		
3) phaseThree:

	- Inputs: this phase recieves multiple inputs (and different input format)- 
		1.  phaseTwo output - we read the Key-Value Phase One Output from the path "dsps202assignment2/$APPID/results/outputPhase2/"
		2. phaseOne output - we read the Key-Value Phase One Output from the path "dsps202assignment2/$APPID/results/outputPhase1/"
	
	- Map:	creates the following Key-Value pairs:
		if receive K-V from phaseTwo output:
			1. <w1, w2, Decade> : - we reverse the order of w1 & w2 and write <w2,w1,Decade> with the values from the previous step

		if receive K-V from phaseOne output:
			1.<W1, *, Decade> we pass it as is with the values from the previous step
	
	
	- Combiner:
		Accumulate the number of appearances for every word or couple per decade.

		
	- Partitioner: Partition all keys by rules set by us.
	
	- Reducer:
		1. Has the same function as the combiner in case that fails.
		Accumulate the number of apperances for every word or couple per decade.
		
		After accumulating, it checks the type of the key and performs the following:
		
		<W1, *, Decade> - Since '*' comes first before every other letter we save the value of cw2.
					so we can pass it to every key from the type <W2,w1>
		
		<W2, W1, Decade>  - since we reversed the order now all the second word is first and we can get cw2 from phase1 output.
		so we already have the values of cw1,cw2,N from Phase2, now we have also cw2 and we can calculate Logliklihood.

		we calculate the Logliklihood ratio by the formula and output the following:
		write to text <W1, W2, Decade> with the value of Logliklihood ratio. 
		this will be saved in "dsps202assignment2/$APPID/results/outputPhase3/"
		
		
		
		statistics:
		
