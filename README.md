# Microsoft Malware Classification Using BLAST.

This project attempts to classify the entries in the Microsoft Malware Classification Challenge dataset using sequence alignment algorithms. 
Following preprocessing scripts were used:

1. FrequencyCountPreprocessing.py - Frequency Counter Model with No Mutation String Used.
2. ContextAlignmentWithMutation.py - A Non-Alignment With Mutation performing script that keeps the hexadecimal strings order intact and implement mutation strings.

3. ContextMutationWithNoMutation.py -  A Non-Alignment with No Mutation performing script that keeps original order of hexadecimal strings intact without performing any mutations.

4. FrequencyCalculationForMutationModel.py - Script to compute mutation strings for 9 different classes. This is to be used right before ContextAlignmentWithMutation.py script.

Following Purge Scripts are used:

1. purge.py - Deletes all the entries in collections

2. purgefrequencycalculation.py - Removes all the entries related to mutations computed for each of the class (Through FrequencyCalculationForMutationModel.py)

Scripts to build the results for presentation:

1. ConfusionVector.py - It builds the result file computing the accuracy of each class.
2. EditDistanceParameters.py - It builds the edit distance results computing the class operations.

CreateResultCollection.py - A stand alone script that creates the result collection

String Processing - A java project that computes sequence comparison between test files and each of the class files.

Presentation - A java project that displays the result
