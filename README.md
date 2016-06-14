# Distributed Systems Programming, Assignment 2
Malachi Cohen (malachic): 203085295  
Lidan Hifi (lidanh): 200298412

---
## Workflow

Our workflow contains 3 steps.

### Step 1: Extract n-grams to pairs
  
*  **Map** takes each record from the corpus and split it.  
        n-gram, year, occurrences, pages and books.  
        For each n-gram, Generates pairs of words with the middle-word and the rest of the ords in the n-gram, while emitting the stop- words and removes all characters which are not words.
For each of those pairs:  
	- Pass the number of instances in the same year and their decade to the reducer.
	- Pass `*,*,<decade> => count` for counting the total per decade.
	- Pass `word, *, <decade>` for counting the total of this word per decade.
 
* **Partitioner** pass a tuple to reducer using `decade % num` of partiotions.

* **Combiner** sums all the instances of same key, while key indicates: `<word1, word2, decade>`

* **Reduce** sums all the instances of same key, while key indicates: `<word1, word2, decade>` (same as combiner)



### Step 2

*  **Map**  
For each key-value, Emit a Map writable according to the following rules:

```
if key == <*, *>: Just pass it forward.  
	emit (<*, *> => <*, *, count>)  
else if key == <w, *>: Just pass it forward.
	emit(<w, *> => <w, *, count>)  
else if key == <w1, w2>:
	emit(w1 => <w1, w2, count>)
```        

* **Partitioner** pass a tuple to reducer using `decade % num` of partitions.
        
* **Comparator** set new comparator which sends the words in reverse order, for getting always the `<w, *>` before all the other `<w1, w2>`.
        
* **Reduce** 

```
if key == <*, *>
	emit (<*, *> => <*, *, count>)  
else if key == <w, *>
	emit (<w, *> => <w, *, count>)  
else if key == <w1, w2>
	emit (w2 => <w1, w2, count>, <w1, *, count>)
```


### Step 3: Calculate PMI
*  **Map**

```
if key == <*, *>:
	emit (<*, *> => <*, *, count>)
else if key == <word, *>:
	keep the value in memory

// now the next pairs will be the words which occured with this word
else if <w1, w2>:
	emit(<w1, w2> => <w1, w2, count>, <w1, *, count>, <w2, *, count>)
```

* **Partitioner** pass a tuple to reducer using `decade % num` of partitions.

* **Reduce** 
each Pair of words now will be with the pattern:   
`<w1, w2> => <w1, w2, count>, <w1, *, count>, <w2, *, count>`
so we have all the parameters for calculating the PMI of `w1` and `w2`.
		
## F-measure:
Given the results, when each line in the pattern `<word1, word2, PMI-Score>`, we load all the result to map  
For threshold from 1 to 10:

for each words pair:

- if it related and the score is greater than threshold: it is a **true positive**
- if it related and the score is lower than threshold: it is a **false positive**
- if it not related and the score is greater than threshold: it is a **true negative**
- if it not related and the score is greater than threshold: it is a **false positive**
    
using the false positive and negatives values, we calc the F-measure.

## K-s: 
We sort all the word pairs in descending order, from the highest value to the lowest. we collect the first K pairs from each decade.


* Note:  
all the Keys word1,word2 include decade. for example: 1900,boy,eat.  
I omitted it for better readability.

## Project Structure
This project is a standard maven project. it contains 6 modules:

1. **word-relatedness-common**- Common utils methods and domain objects (`WordPair` for example).
2. **word-relatedness-job1**- First step objects, including the mapper, reducer, and the wiring of the job.
3. **word-relatedness-job2**- Same as `word-relatedness-job1`, but for step 2.
4. **word-relatedness-job3**- Same as `word-relatedness-job1`, but for step 3.
5. **word-relatedness-local**- Runner for local environment (Hosted hadoop cluster).
6. **word-relatedness-emr**- Runner for AWS Elastic MapReduce environment.

We separate the project like that in order to create runnable JAR for each of the steps.

#### Creating runnable JAR files
In order to create the runnable JARs, just run the following command: 

```shell
$  mvn package
```

The runnable jars for each of the jobs and the runners will be created under `target` directory.

