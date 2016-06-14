# Distributed Systems, Assignment 2
Malachi Coehn : 203085295  
Lidan Hifi: 

---
## Workflow

- First Map reduce: 
  - 

    * ### Map:  
        Takes each record from the corpus and split it.  
        n-gram, year, occurrences, pages and books.  
        For each n-gram, Generates pairs of words with the middle-word and the rest of the ords in the n-gram, while emitting the stop- words and removes all characters which are not words.
        For each of those pairs:  
                -  Pass the number of instances in the same year and their decade to the reducer.
                - Pass *,*,decade --\> count for  counting the total per decade.
                - Pass each word of the pair with * for counting the total of this word per decade.
 
   * ### Partitioner :   
        Pass a tuple to reducer using decade % num of partiotions

    * ### Combiner :   
        Sums all the instances of same key, while key indicates: \<word1, word2, decade\>

    * ### Reduce:  
        Sums all the instances of same key, while key indicates: \<word1, word2, decade\> (same as combiner)

- Second Map reduce: 
  --

    * ### Map :  
        For each key-value:   
        Emit a Map writable according to the following rules:  
        if key == * - *: Just pass it forward.  
        &nbsp;&nbsp;emit (* - *| \<* - *, count\>)  
        else if key == word - *: Just pass it forward.    
        &nbsp;&nbsp;emit(word - *| \<word- *, count\>)  
        else if key == word1,word2  
        &nbsp;&nbsp;emit(word1| \<word1-word2, count\>)  
        
    * ### Partitioner :   
        Pass a tuple to reducer using decade % num of partiotions
        
    * ### Comparator :   
        Set new comparator which sends the words in reverse order, for getting always the word1 - * before all the other word1 - word2
        
    * ### Reducer :   
       	if key == * - *  
        &nbsp;&nbsp;emit (* - *==\> \<* - *, count\>)  
    	else if key == word- *  
    	&nbsp;&nbsp;emit(word - * ==\> \<word- *, count\>)  
    	else if key == word1-word2  
    	&nbsp;&nbsp;emit(word2 ==\> \<word1-word2, count\>, \<word1- *, count\>)  

	


- Third Map reduce: 
  --
    * ### Partitioner :   
        Pass a tuple to reducer using decade % num of partiotions

    * ### Map :  
		if key == *-*:  
		&nbsp;&nbsp;emit (* - * ==\> \<* - *, count\>)
		else if 
		if word *:  
		&nbsp;&nbsp;keep th value in memory
		&nbsp;&nbsp;now the next pairs will be the words which occured with this word
	    else if word1,word2:  
		&nbsp;&nbsp;emit(word1,word2 ==\> \<word1-word2, count\>, \<word1- *, count\>, \<word - *, count\>)

    * ### Reducer :
		each Pair of words now will be with the pattern:   
		word1,word2==\> \<word1-word2, count\>, \<word1-*, count\>, \<word-*, count\>
		So we have all the parameters for calculating the PMI of word1 and word2 
		
- F-measure: 
  --
    Given the results, when each line in the pattern \<word1, word2    PMI-Score\>, we load all the result to map  
For threshold from 1 to 10:  
    for each words pair:
    * if it related and the score is greater than threshold : it is a true positive  
    * if it related and the score is lower than threshold : it is a false positive  
    * if it not related and the score is greater than threshold : it is a true negative  
    * if it not related and the score is greater than threshold : it is a false positive
    
    using the false positive and negatives values, we calc the F-measure.

- K-s: 
  --
    We sort all the word pairs from the highest value to the lowest.  
    we collect the first K pairs from each decade.

--- 
* Note:  
all the Keys word1,word2 include decade. for example: 1900,boy,eat.  
I ommited it for better readability.