# challenge-globant

I made this challenge using a AWS stack with the following services:
 - Glue
 - S3
 - RDS MySQL
 - Lambda
 - API Gateway
 - VPC
 - Secrets Manager
 - IAM

With Glue (+S3+RDS) I resolved points 1, 3 and 4 of Challenge #1 and with Lambda I resolved point 2 of Challenge #1 and all the Challenge #2.

I choose that Stack because I have more familiarity, in general, in that services.

Unfortunately, I have not been able to make API Rest done (but I could tested that from API Gateway). Also I am not proficient enough in Docker. Lastly, when I tried to make the reports on Quicksight I found myself with AWS trying to charge me for a new account.

Regarding Glue, scripts are in: 
 - https://github.com/fernandodanielmartinez/challenge-globant/tree/main/job-load-historical-data (done with Pandas)
 - https://github.com/fernandodanielmartinez/challenge-globant/tree/main/job-backup-entities-data (done with PySpark because the flexibility of Avro libs)
 - https://github.com/fernandodanielmartinez/challenge-globant/tree/main/job-restore-backup-entity (done with PySpark because the flexibility of Avro libs)

Regarding Lambda, scripts are in:
 - https://github.com/fernandodanielmartinez/challenge-globant/tree/main/load-api-rest-data (done with Python)
 - https://github.com/fernandodanielmartinez/challenge-globant/tree/main/get-totals-mean-hired-employees (done with Python)

S3 buckets structure:

  ![image](https://user-images.githubusercontent.com/37267326/221586308-2b8e158a-049f-4449-9e80-e524a4be94b2.png)

  ![image](https://user-images.githubusercontent.com/37267326/221586364-c6a6c863-ee19-4895-830d-96d4160cccea.png)

  ![image](https://user-images.githubusercontent.com/37267326/221586390-beabca76-dbb9-4f68-83ad-ed2d937c1419.png)

  ![image](https://user-images.githubusercontent.com/37267326/221586402-66b41036-963d-4a43-9b6d-7b3fb7650a05.png)

  ![image](https://user-images.githubusercontent.com/37267326/221586423-9d51d72c-a0c0-4af8-a922-4d551fa9f46e.png)

  ![image](https://user-images.githubusercontent.com/37267326/221586424-58b6ff86-b308-41e0-80a5-75ccf7b73d40.png)
  
  ![image](https://user-images.githubusercontent.com/37267326/221586478-64097d1d-5074-4bdd-a6bb-f5f1fa8e2ec1.png)

 API gateway:
 
  ![image](https://user-images.githubusercontent.com/37267326/221586551-5c5b9502-9ab3-469f-9510-65f15a485f13.png)

  ![image](https://user-images.githubusercontent.com/37267326/221586579-eee4fc39-7f74-4992-8b8d-77ecbffd40e1.png)

 RDS:
 
  ![image](https://user-images.githubusercontent.com/37267326/221586643-209124b4-04f0-4f41-a8ee-8bbbc5652faa.png)


  
