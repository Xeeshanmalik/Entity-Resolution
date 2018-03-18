## Entity Resolution ##

"Entity Resolution" is the problem of identifying which records in a database
represent the same entity. When dealing with user data, it is often difficult to
control the quality of the data inputted into the system. The poor quality of the
data may be characterized by:-

1) Duplicated records
2) Records that link to the same entity across different data sources
3) Data fields with more than one possible representation (e.g. "P&G" and 
   "Procter and Gamble")
   
   
**Configuration**

1) Install conda https://conda.io/docs/user-guide/install/index.html
2) Execute ``conda env create`` at the project root folder only for once.


To resolve deduplicate python API is used for active learning de-duplication.
This API could be considered as one of the potential choice for initial investigation 
when you have data that can contain multiple records that can all refer to the same 
entity across datasets. It is for sure harder for this API to compete with latest deep 
networks.

**Run Code**

````
chmod +x ./run-training.sh
./run-training.sh
````

**Description of Steps**

1) Basic noise removal in both the datasets.

2) Initialize a dedupe object with the field definition. The field definition
   includes key features from both the datasets assumed to be useful to
   solve the problem. Initially title, author, venue and year is initialized.

3) The fields are initialized inside a record link object which is specifically
   used for record link matching between two datasets.

4) The console level training is embedded in this implementation. which runs on
   the random sample extracted from both the datasets. The more we label data 
   the better we can expect the accuracy in the end. The best way would be to 
   construct training from already linked datasets which can be easily done in 
   the future with this API for better performance.

5) After completion of training it will be saved locally and in the next execution 
   predictions would be generated from the trained model.

6) Two news files will be created after training finishes 1) data_matching_learned_settings
   and  2) data_matching_training.json. The setting files contains the data model
   and predicates to a file object. The second file contains labeled examples to a file
   object.

7) After training linker.match() runs to find probabilistic references between two
   datasets based on the defined threshold.

8) Finally scripts to find links between both the datasets will be exectued based on 
   the predicted clusters from the algorithm.
   
9) In the end, the output will be saved in the file named DBLP_Scholar_perfectMapping_[Zeeshan_Malik].csv


**Assumptions**:-

The training for the complete two files is a bit time consuming. For fast check there are subsets from 
both the files ending with test. The code is configured with it. To run training on original files you
need to pass the name of both the files as parameter to the code.



 

 
 
