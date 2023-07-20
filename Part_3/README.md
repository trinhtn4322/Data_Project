# Final Part
### In this section, we will go deeper into the goal of the problem, which is to apply the model to the data we have collected, analyzed and processed in the previous 2 parts.
A brief overview of the data:
- Includes 1 target variable: CHANCE_OF_PRECIPITATION
- And 10 different features
![image](https://github.com/trinhtn4322/Data_Project/assets/115331941/8cef192d-9a8b-4e0d-a389-21677d1dedb1)

This section consists of 3 main contents
- Step 1: Prepare data for model building
- Step 2: Build SVM and Neural Network model
- Step 3: Evaluate and compare 2 models

Summary of the problem
This is a binary classification problem
- Target includes 2 variables 0 and 1 representing the probability of rain
![image](https://github.com/trinhtn4322/Data_Project/assets/115331941/baea12f4-0e0e-4112-a2b8-55badfeb2cc3)

  
Since it is a binary classification problem, we will choose the traditional machine learning model, SVM, which is quite famous and achieves good accuracy in many different classification problems.

***Support Vector Machine (SVM)*** is a machine learning algorithm used for classification and regression problems. The basic idea of SVM is to find a boundary that divides the data of two classes so that the distance from the data points closest to the boundary to the boundary is maximized. The data points closest to the edge are called Support Vectors. SVM finds this boundary by optimizing an objective function such that the boundary is maximal.


***Neural Network model*** that we will use will be a relatively simple model with Dense classes (Tensoflow library). The structure includes:
- Dense layer 1: There are 64 nodes with ReLU activation function, taking input as 10 features.
- Layer Dense 2: There are 32 nodes with ReLU . activation function
- Output Dense layer: There is 1 node with sigmoid activation function, used to make probabilistic predictions for binary problems.

**After analyzing and evaluating, I found that for a small amount of data and a simple binary classification model, the victory belongs to the traditional machine learning model SVM.**
