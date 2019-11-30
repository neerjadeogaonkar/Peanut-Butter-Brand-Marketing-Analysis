
/* Assigning the Project Library */

LIBNAME pea "C:\Users\nxd180003\Desktop\peanbutr";

/* ---------------------- IMPORT CODE SNIPPETS --------------------------- */


/* Im porting Peanut product file */

proc import datafile = 'C:\Users\nxd180003\Desktop\peanbutr\peanut_L5.csv'
 out = peanut REPLACE;
run;


Data pea.peanut_prod;
set work.peanut;
run;


/* Importing peanutbutr panel data */

proc import datafile = 'C:\Users\nxd180003\Desktop\peanbutr\peanut_panel.csv'
 out = peanut_panel REPLACE;
run;

Data pea.peanut_panel;
set work.peanut_panel;
run;



/* Importing the sales data */ 

DATA peanut_groc;
   INFILE 'C:\Users\nxd180003\Desktop\peanbutr\peanbutr_groc_1114_1165';
   INPUT IRI_KEY 1-8  WEEK 9-13  SY 14-16 GE 17-19 VEND 20-25  ITEM 26-31  UNITS 32-37 DOLLARS 38-46  F $ 47-51   D 52-53 PR;
RUN;

Data pea.peanut_groc;
set work.peanut_groc;
IF IRI_KEY = '.' then DELETE;
run;


/* Importing the weeks reference table */


proc import datafile = 'C:\Users\nxd180003\Desktop\peanbutr\week_ref.csv'
 out = week_ref REPLACE;
run;

Data pea.week_ref;
set work.week_ref;
run;


/*Importing Store Information */


DATA store;
   INFILE 'C:\Users\nxd180003\Desktop\peanbutr\Delivery_Stores';
   INPUT IRI_KEY 1-8 OU $ 9-11 EST_ACV 12-20  Market_Name $ 21-45 Open 46-50 Clsd 51-55 MskdName $;
RUN;

Data pea.store;
set work.store;
IF IRI_KEY = '.' then DELETE;
run;




/* Importing Demography file */

proc import datafile = 'C:\Users\nxd180003\Desktop\peanbutr\Panel_demo.csv'
 out = demo REPLACE;
run;

Data pea.demo;
set work.demo;
run;


/* ---------------------------- DATA CLEANING AND ANALYSIS --------------------- */


%MACRO IMPORT(OUT=);

DATA &OUT._1;
SET &OUT.;

IF SY="0" 
THEN SY="00";

IF SY="6" 
THEN SY="06";

IF SY="7" 
THEN SY="07";

RUN;

DATA &OUT._1;
SET &OUT._1;

IF GE="1"
THEN GE="01";

IF GE="2"
THEN GE="02";

IF GE="3"
THEN GE="03";

IF GE="4"
THEN GE="04";

IF GE="5"
THEN GE="05";

RUN;

DATA &OUT._1;
SET &OUT._1;

IF LENGTH(ITEM)=2 THEN ITEM="000"||TRIM(ITEM);

IF LENGTH(ITEM)=3 THEN ITEM="00"||TRIM(ITEM);

IF LENGTH(ITEM)=4 THEN ITEM="0"||TRIM(ITEM);

RUN;

DATA &OUT._1;
SET &OUT._1;

IF LENGTH(VEND)=2 THEN VEND="000"||TRIM(VEND);

IF LENGTH(VEND)=3 THEN VEND="00"||TRIM(VEND);

IF LENGTH(VEND)=4 THEN VEND="0"||TRIM(VEND);

RUN;

PROC CONTENTS DATA=&OUT._1;
RUN;

%MEND IMPORT;

%IMPORT(OUT=pea.peanut_groc);
%IMPORT(OUT=pea.peanut_prod);



/* Combine tables peanut_groc and peanut */

PROC SQL;
CREATE TABLE pea.sales_1 AS  (
SELECT 
a.*,
b.* 
FROM pea.peanut_groc_1 as a
JOIN pea.peanut_prod_1 as b on a.item = b.item and a.vend = b.vend and a.sy = b.sy and a.ge = b.ge );
quit();


PROC SQL;
CREATE TABLE pea.Skippy AS SELECT *
FROM pea.sales_1
WHERE L5 LIKE '%SKIPPY%';
QUIT();


/* Filtering for only those stores that have 52 weeks of data */

PROC SQL;
CREATE TABLE pea.Sales_2 AS
SELECT IRI_KEY,COUNT(DISTINCT WEEK) AS DISTINCT_WEEKS FROM pea.Skippy
GROUP BY IRI_KEY
HAVING DISTINCT_WEEKS=52;
QUIT;


PROC SQL;
CREATE TABLE pea.Sales_3 AS
SELECT * FROM pea.Skippy WHERE IRI_KEY IN
(SELECT IRI_KEY FROM pea.Sales_2 );
QUIT;


/* Calculating Price per unit sold and volume in oz of all units sold*/

DATA Pea.Sales_4;
SET Pea.Sales_3;

TOT_VOL = VOL_EQ*16*UNITS;
PRICE_PER_OZ = DOLLARS/TOT_VOL;

RUN;

/* Calculating the price per oz of each item */


DATA Pea.Sales_Calc_1;
SET Pea.Sales_4;
RUN;

data Pea.Sales_Calc_1;
  set Pea.Sales_Calc_1;
  length prod_id $17;
  prod_id=put(sy,z2.)||'-'||put(ge,z2.)||'-'||put(vend,z5.)||'-'||put(item,z5.);
run;




/****************************creating dataset with only required columns*****/
PROC SQL;
CREATE TABLE Pea.Peanut_Calc AS
SELECT IRI_KEY,WEEK,UNITS,TOT_VOL,DOLLARS,F,D,PR,PROD_ID,PRICE_PER_OZ FROM Pea.Sales_Calc_1;
QUIT;

/* DataSet checks */

PROC PRINT DATA=Pea.Peanut_Calc(OBS=10);
RUN;

PROC FREQ DATA=Pea.Peanut_Calc;
TABLE F;
RUN;

PROC FREQ DATA=Pea.Peanut_Calc;
TABLE D;
RUN;

PROC FREQ DATA=Pea.Peanut_Calc;
TABLE PR;
RUN;

PROC CONTENTS DATA=Pea.Peanut_Calc;
RUN;



PROC CONTENTS DATA=Pea.Peanut_Calc;
RUN;


/* Clean Feature, Display and PR columns */

DATA Pea.Peanut_Calc;
SET Pea.Peanut_Calc;

IF F='NONE' THEN F_1=0;
ELSE F_1=1;

IF D='0' THEN D_1=0;
ELSE D_1=1;

IF PR='0' THEN PR_1=0;
ELSE PR_1=1;

RUN;




/**********************CREATING GROUPED DATASET FROM Peanut_Calc BY GROUPING DATA BY IRI_KEY, WEEK COMBINATION***/




/* Creating grouped dataset at Store,Week,UPC level */
proc sql;
create table pea.Peanut_Grouped as select iri_key,week,prod_id,sum(units) as tot_units,sum(tot_vol) as sum_vol,sum(dollars) as tot_dollars,
sum(f_1) as tot_f, sum(d_1) as tot_d,sum(pr_1) as tot_pr, avg(price_per_oz) as avg_ppz
from Pea.Peanut_Calc
group by iri_key,week,prod_id; quit;

/* Exporting grouped dataset */
proc export data=pea.Peanut_Grouped
outfile="C:\Users\nxd180003\Desktop\peanbutr\Peanut_Grouped.csv" dbms=CSV; run;



/* Creating weighted price, feature, display and price reduction variables for panel regression */

proc sql;
create table pea.Peanut_Weekly as
select iri_key,week,sum(units) as tot_iri_week_units, sum(tot_vol) as tot_iri_week_vol
from pea.peanut_calc
group by iri_key,week; quit;


PROC SQL;
CREATE TABLE pea.Peanut_Grouped_1 AS
SELECT a.*, b.* FROM 
(SELECT * FROM pea.Peanut_Grouped) AS a
LEFT JOIN
(SELECT * FROM pea.Peanut_Weekly) AS b
ON a.IRI_KEY=b.IRI_KEY AND a.WEEK=b.WEEK;
QUIT;


DATA pea.Peanut_Grouped_1;
SET pea.Peanut_Grouped_1;

W_PRICE = AVG_PPZ*(SUM_VOL/TOT_IRI_WEEK_VOL);

IF TOT_F=0 THEN W_FEATURE=0;
ELSE W_FEATURE=1*(SUM_VOL/TOT_IRI_WEEK_VOL);

IF TOT_D=0 THEN W_DISPLAY=0;
ELSE W_DISPLAY=1*(SUM_VOL/TOT_IRI_WEEK_VOL);

IF TOT_PR=0 THEN W_PROMOTION=0;
ELSE W_PROMOTION=1*(SUM_VOL/TOT_IRI_WEEK_VOL);

RUN;



/* Creating final panel dataset for regression */

PROC SQL;
CREATE TABLE Pea.Peanut_Final AS
SELECT IRI_KEY,WEEK,SUM(TOT_UNITS) AS SUM_UNITS, SUM(SUM_VOL) AS SUM_VOL, SUM(TOT_DOLLARS) AS SUM_DOLLARS, SUM(W_PRICE) AS SUM_W_PRICE,
SUM(W_FEATURE) AS SUM_W_FEATURE, SUM(W_DISPLAY) AS SUM_W_DISPLAY, SUM(W_PROMOTION) AS SUM_W_PROMOTION
FROM Pea.Peanut_Grouped_1
GROUP BY IRI_KEY,WEEK;
QUIT;

/***** Peanut_Final - Panel Regression******/

DATA Pea.Peanut_Pan;
SET Pea.Peanut_Final;
RENAME SUM_UNITS=UNITS_SOLD SUM_VOL=VOL_SOLD SUM_DOLLARS=DOLLARS SUM_W_PRICE=WEIGHTED_PRICE 
SUM_W_FEATURE=WEIGHTED_FEATURE SUM_W_DISPLAY=WEIGHTED_DISPLAY SUM_W_PROMOTION=WEIGHTED_PRICE_REUDCTION;
RUN;

PROC PRINT DATA=Pea.Peanut_Pan (OBS=50);
RUN;

/****** Non-Linearity check******/

PROC SGPLOT DATA=Pea.Peanut_Pan;
SCATTER X=WEIGHTED_PRICE Y=VOL_SOLD;
RUN;

/*******Multicollinearity check*****/

PROC CORR DATA=Pea.Peanut_Pan;
VAR VOL_SOLD DOLLARS WEIGHTED_PRICE WEIGHTED_FEATURE WEIGHTED_DISPLAY WEIGHTED_PRICE_REUDCTION;
RUN;

/* Ordinary Regression Code */

PROC REG DATA=Pea.Peanut_Pan;
MODEL VOL_SOLD = WEIGHTED_PRICE WEIGHTED_FEATURE WEIGHTED_DISPLAY WEIGHTED_PRICE_REUDCTION/ VIF COLLIN;
RUN;

/* Panel Regression Code - Without weights */

PROC PANEL DATA = Pea.Peanut_Pan;
ID IRI_KEY WEEK;       
MODEL VOL_SOLD = WEIGHTED_PRICE WEIGHTED_FEATURE WEIGHTED_DISPLAY WEIGHTED_PRICE_REUDCTION/FIXTWO RANTWO;    
RUN;

/*  Hausman Test is significant, Therefore the hypothesis can be rejected */

/*Introducing Interaction Effect*/


DATA Pea.Peanut_Pan;
SET Pea.Peanut_Pan;
WT_PRICESQ = WEIGHTED_PRICE * WEIGHTED_PRICE;
WT_FEATURESQ = WEIGHTED_FEATURE * WEIGHTED_FEATURE;
WT_DISPLAYSQ = WEIGHTED_DISPLAY * WEIGHTED_DISPLAY;
WT_PRREDSQ = WEIGHTED_PRICE_REUDCTION * WEIGHTED_PRICE_REUDCTION;
WTPRICE_WTFEATURE = WEIGHTED_PRICE * WEIGHTED_FEATURE;
WTPRICE_WTDISP = WEIGHTED_PRICE * WEIGHTED_DISPLAY;
WTPRICE_WTPROMO = WEIGHTED_PRICE * WEIGHTED_PRICE_REUDCTION;
WTFEATURE_WTDISP = WEIGHTED_FEATURE * WEIGHTED_DISPLAY;
WTFEATURE_WTPRRED = WEIGHTED_FEATURE * WEIGHTED_PRICE_REUDCTION;
WTDISP_WTPRRED = WEIGHTED_DISPLAY * WEIGHTED_PRICE_REUDCTION;
WTDISP_WTFEATURE_WTPRRED = WEIGHTED_DISPLAY * WEIGHTED_FEATURE * WEIGHTED_PRICE_REUDCTION;
RUN;


PROC PANEL DATA = Pea.Peanut_Pan;
ID IRI_KEY WEEK;       
MODEL VOL_SOLD = WEIGHTED_PRICE WT_PRICESQ WEIGHTED_DISPLAY WEIGHTED_FEATURE 
WEIGHTED_PRICE_REUDCTION WTFEATURE_WTPRRED WTDISP_WTPRRED WTDISP_WTFEATURE_WTPRRED/FIXTWO RANTWO;    
RUN;

PROC MEANS DATA = Pea.Peanut_Pan;
VAR VOL_SOLD WEIGHTED_PRICE WEIGHTED_DISPLAY WEIGHTED_FEATURE WEIGHTED_PRICE_REUDCTION;
RUN;



/************************************* RFM ANALYSIS *******************************************/

/* Recreating the dataset Peanut_Panel with UPC code assigned the correct datatype  */

DATA pea.Peanut_Panel_2;
SET pea.Peanut_Panel;
UPC=PUT(COLUPC,z13.);
RUN;


/* Recreating the product table with UPC code */

DATA pea.Peanut_Prod_2;
  SET pea.Peanut_Prod;
  upc2=PUT(SY,Z2.)||PUT(GE,Z1.)||PUT(VEND,Z5.)||PUT(ITEM,Z5.);
RUN;

PROC SQL;
CREATE TABLE pea.Rfm_1 AS SELECT *
FROM pea.Peanut_Panel_2 a LEFT JOIN
pea.Peanut_Prod_2 b ON a.upc=b.upc2
where L5 LIKE '%SKIPPY%';



/* Recreating the product table with UPC code */

DATA pea.Week_Ref_2;
SET pea.Week_Ref;
RUN;


PROC SQL;
CREATE TABLE pea.Panel_Final AS
SELECT * FROM
(SELECT * FROM pea.Rfm_1) AS A 
inner JOIN
(SELECT * FROM pea.Week_Ref_2) AS B
ON A.WEEK=B.IRI_WEEK;
QUIT();

/* Assigning the last week in data */

DATA pea.Panel_Final;
SET pea.Panel_Final;
LAST_WEEK='30DEC2001'D;
FORMAT LAST_WEEK DDMMYY10.;
run;



PROC SQL;
CREATE TABLE pea.Rfm_Final AS
SELECT PANID, SUM(DOLLARS) AS MONETARY,COUNT(WEEK) AS FREQUENCY,MAX(WEEK) AS LAST_PURCHASE,
MIN(1165-WEEK) AS RECENCY
FROM pea.Panel_Final
GROUP BY PANID
HAVING FREQUENCY>1;
QUIT;

/** Monetary and Frequqency have very high correlation so we can use any one*/
PROC CORR DATA=pea.Rfm_Final;
VAR MONETARY FREQUENCY RECENCY;
RUN;

PROC MEANS DATA=pea.Rfm_Final MIN P20 P40 P60 P80 MAX;
VAR MONETARY RECENCY;
OUTPUT OUT=pea.Cust_Percentile MIN= P20= P40= P60= P80= MAX=/ AUTONAME;
RUN;


/*CREATING CUSTOMER SEGMENTS*/
DATA pea.Rfm_Final;
SET pea.Rfm_Final;
ID=1;

DATA pea.Cust_Percentile;
SET pea.Cust_Percentile;
ID = 1;
RUN;

PROC SQL;
CREATE TABLE pea.Panel_With_Segments AS
SELECT * FROM
(SELECT * FROM pea.Rfm_Final) AS A 
LEFT JOIN
(SELECT * FROM pea.Cust_Percentile) AS B
ON A.ID=B.ID;

/* Creating Segments */

DATA pea.Panel_With_Segments_2 (KEEP=PANID MONETARY FREQUENCY RECENCY SEGMENT );
SET pea.Panel_With_Segments;
IF (MONETARY > MONETARY_P80) THEN SEGMENT=1;
ELSE IF (MONETARY > MONETARY_P60 & RECENCY > RECENCY_P80) THEN SEGMENT=2;
ELSE SEGMENT=0;
RUN;


/* ------ USING DEMOGRAPHICS DATA TO UNDERSTAND THE TOP 20% OF OUR CUSTOMERS ------ */

Data pea.demo_2;
set pea.demo;
run;


DATA pea.demo_3 (KEEP = PANID  INCOME Family_Size RESIDENT_TYPE AGE_MALE EDUC_MALE OCC_MALE MALE_WORK_HR AGE_FEMALE EDUC_FEMALE 
OCC_FEMALE FEMALE_WORK_HR NUM_DOGS NUM_CATS CHILD_AGE MARITAL_STATUS);
SET pea.demo_2(RENAME = (Panelist_ID = PANID Combined_Pre_Tax_Income_of_HH = INCOME Family_Size = Family_Size 
Type_of_Residential_Possession = RESIDENT_TYPE Age_Group_Applied_to_Male_HH	= AGE_MALE Education_Level_Reached_by_Male = EDUC_MALE 
Occupation_Code_of_Male_HH = OCC_MALE Male_Working_Hour_Code = MALE_WORK_HR Age_Group_Applied_to_Female_HH = AGE_FEMALE 
Education_Level_Reached_by_Femal = EDUC_FEMALE Occupation_Code_of_Female_HH = OCC_FEMALE Female_Working_Hour_Code = FEMALE_WORK_HR 
Number_of_Dogs = NUM_DOGS Number_of_Cats = NUM_CATS Children_Group_Code = CHILD_AGE Marital_Status = MARITAL_STATUS));
RUN;


PROC SQL;
CREATE TABLE pea.demo_4 AS
SELECT * FROM pea.demo_3 WHERE Family_Size <> 0 AND RESIDENT_TYPE <> 0 AND AGE_MALE <> 7 AND AGE_MALE <> 0 AND EDUC_MALE <> 9 AND EDUC_MALE <> 0 AND OCC_MALE <> 11 AND MALE_WORK_HR <> 7 AND 
AGE_FEMALE <> 7 AND AGE_FEMALE <> 0 AND EDUC_FEMALE <> 9 AND EDUC_FEMALE <> 0 AND OCC_FEMALE <> 11 AND FEMALE_WORK_HR <> 7 AND MARITAL_STATUS <> 0 AND CHILD_AGE <> 0 ;
QUIT;


DATA pea.demo_5;
SET pea.demo_4;
PETS_TOTAL=NUM_CATS+NUM_DOGS;RUN;


DATA pea.demo_6;
SET pea.demo_5;
IF Family_Size in (4,5,6)THEN FAM_SIZE_L=1 ; ELSE FAM_SIZE_L=0;
IF Family_Size in (1,2,3) THEN FAM_SIZE_R=1 ; ELSE FAM_SIZE_R=0;

IF INCOME IN (1,2,3,4) THEN FAM_INCOME_L=1 ; ELSE FAM_INCOME_L=0;
IF INCOME IN (5,6,7,8) THEN FAM_INCOME_M=1 ; ELSE FAM_INCOME_M=0;
IF INCOME IN (9,10,11,12) THEN FAM_INCOME_H=1 ; ELSE FAM_INCOME_H=0;

IF AGE_MALE IN (1) THEN AGE_MY=1 ; ELSE AGE_MY=0;
IF AGE_MALE IN (2,3,4) THEN AGE_MM=1 ; ELSE AGE_MM=0;
IF AGE_MALE IN (5,6) THEN AGE_ME=1 ; ELSE AGE_ME=0;

IF AGE_FEMALE IN (1) THEN AGE_FY=1 ; ELSE AGE_FY=0;
IF AGE_FEMALE IN (2,3,4) THEN AGE_FM=1 ; ELSE AGE_FM=0;
IF AGE_FEMALE IN (5,6) THEN AGE_FE=1 ; ELSE AGE_FE=0;

IF EDUC_MALE IN (1,2,3) THEN EDUC_MS=1 ; ELSE EDUC_MS=0;
IF EDUC_MALE IN (4,5,6) THEN EDUC_MC=1 ; ELSE EDUC_MC=0;
IF EDUC_MALE IN (7,8) THEN EDUC_MG=1 ; ELSE EDUC_MG=0;

IF EDUC_FEMALE IN (1,2,3) THEN EDUC_FS=1 ; ELSE EDUC_FS=0;
IF EDUC_FEMALE IN (4,5,6) THEN EDUC_FC=1 ; ELSE EDUC_FC=0;
IF EDUC_FEMALE IN (7,8) THEN EDUC_FG=1 ; ELSE EDUC_FG=0;

IF OCC_MALE IN (1,2,3)  THEN OCC_MWH=1; ELSE OCC_MWH=0;
IF OCC_MALE IN (4,5)  THEN OCC_MWL=1; ELSE OCC_MWL=0;
IF OCC_MALE IN (6,7,8,9) THEN OCC_MB=1; ELSE OCC_MB=0;
IF OCC_MALE IN (10,13) THEN OCC_MNO=1; ELSE OCC_MNO=0;

IF OCC_FEMALE IN (1,2,3) THEN OCC_FWH=1; ELSE OCC_FWH=0;
IF OCC_FEMALE IN (4,5) THEN OCC_FWL=1; ELSE OCC_FWL=0;
IF OCC_FEMALE IN (6,7,8,9) THEN OCC_FB=1; ELSE OCC_FB=0;
IF OCC_FEMALE IN (10,13) THEN OCC_FNO=1; ELSE OCC_FNO=0;


IF CHILD_AGE IN (1,2,3) THEN CHILD_1=1; ELSE CHILD_1=0;
IF CHILD_AGE IN (4,5,6) THEN CHILD_2=1; ELSE CHILD_2=0;
IF CHILD_AGE IN (7) THEN CHILD_3=1; ELSE CHILD_3=0;
IF CHILD_AGE IN (8) THEN CHILD_0=1; ELSE CHILD_0=0;

IF PETS_TOTAL=0 THEN PETS=1; ELSE PETS=0;

RUN;




DATA pea.demo_7 (DROP = INCOME Family_Size RESIDENT_TYPE AGE_MALE EDUC_MALE OCC_MALE MALE_WORK_HR AGE_FEMALE EDUC_FEMALE OCC_FEMALE FEMALE_WORK_HR CHILD_AGE MARITAL_STATUS PETS_TOTAL NUM_DOGS NUM_CATS);
SET pea.demo_6;
RUN;

PROC SQL;
CREATE TABLE pea.Demo_Final AS
SELECT * FROM
(SELECT * FROM pea.Panel_With_Segments_2) AS a 
INNER JOIN
(SELECT * FROM pea.demo_7) AS b
ON a.PANID=b.PANID
WHERE a.SEGMENT=1;
QUIT;

PROC MEANS DATA=pea.Demo_Final;RUN;



/* ----------------- Running Logistic Regression ------------------- */


PROC SQL;
CREATE TABLE pea.Log_Reg AS 
SELECT *
FROM pea.Peanut_Panel_2 a LEFT JOIN
pea.Peanut_Prod_2 b ON a.upc=b.upc2;
QUIT();


PROC SQL;
CREATE TABLE pea.Log_Reg_Data AS SELECT *
FROM pea.Log_Reg a LEFT JOIN
pea.demo_4 b ON a.PANID=b.PANID;
QUIT();


DATA pea.Log_Reg_Data_1 (KEEP = L5 INCOME Family_Size CHILD_AGE);
SET pea.Log_Reg_Data;
RUN;


DATA pea.Log_Reg_Data_2;
SET pea.Log_Reg_Data_1;
IF FIND(L5,'SKIPPY','i') GE 1 THEN DECISION = 1; 
ELSE IF FIND(L5,'JIF','i') GE 1 THEN DECISION=2;
ELSE IF FIND(L5,'PETER','i') GE 1 THEN DECISION =3;
ELSE IF FIND(L5,'PRIVATE','i') GE 1 THEN DECISION =4;

RUN;




DATA pea.Log_Reg_Data_3;
SET pea.Log_Reg_Data_2;
IF DECISION = "." THEN DELETE;
IF income = "." THEN DELETE;
RUN;



DATA pea.Log_Reg_Data_4;
SET pea.Log_Reg_Data_3;
IF Family_Size in (4,5,6)THEN FAM_SIZE=2 ; ELSE FAM_SIZE=1;

IF INCOME IN (1,2,3,4) THEN FAM_INCOME=1;
IF INCOME IN (5,6,7,8) THEN FAM_INCOME=2 ;
IF INCOME IN (9,10,11,12) THEN FAM_INCOME=3;

IF CHILD_AGE IN (1,2,3) THEN CHILDCOUNT=1;
IF CHILD_AGE IN (4,5,6) THEN CHILDCOUNT=2;
IF CHILD_AGE IN (7) THEN CHILDCOUNT=3;
IF CHILD_AGE IN (8) THEN CHILDCOUNT=0;

RUN;

PROC LOGISTIC DATA = pea.Log_Reg_Data_4;
CLASS DECISION (REF = "1") / PARAM = ref;
MODEL DECISION = FAM_INCOME CHILDCOUNT FAM_SIZE / LINK = glogit;
RUN;




/* ----------------------------- MDC ------------------------ */

/*** Reloading Peanutbutter panel Data ***/


DATA pea.Peanut_Panel_1;
SET pea.Peanut_Panel;
UPC=PUT(COLUPC,Z13.);
RUN;


DATA pea.Peanut_Panel_1(DROP = OUTLET);
SET pea.Peanut_Panel_1(RENAME = (UNITS = P_UNITS DOLLARS = P_DOLLARS));
RUN;

DATA pea.Peanut_Groc_1;
SET pea.Peanut_Groc;
IF IRI_KEY = '.' THEN DELETE;
RUN;


DATA pea.Peanut_Groc_1;
SET pea.Peanut_Groc_1;
UPC = PUT(SY,z2.)||PUT(GE,z1.)||PUT(VEND,z5.)||PUT(ITEM,z5.);
RUN;


DATA pea.Peanut_Groc_1(DROP = SY GE VEND ITEM );
SET pea.Peanut_Groc_1;
RUN;


PROC SQL;
CREATE TABLE pea.Ps_Peanut AS
SELECT * FROM
(SELECT * FROM pea.Peanut_Groc_1) AS a 
INNER JOIN
(SELECT * FROM pea.Peanut_Panel_1) AS b
ON a.IRI_KEY=b.IRI_KEY AND a.WEEK = b.WEEK AND a.UPC = b.UPC;
QUIT;


/*** Reloading Peanutbutter product Data ***/


DATA pea.Peanut_Prod_1;
SET pea.Peanut_Prod;
UPC = PUT(SY,z2.)||PUT(GE,z1.)||PUT(VEND,z5.)||PUT(ITEM,z5.);
RUN;

PROC SQL;
CREATE TABLE pea.Peanut_Prod_2 AS
SELECT * FROM pea.Peanut_Prod_1 
WHERE L5 LIKE '%JIF%' OR L5 LIKE '%SKIPPY%' OR L5 LIKE '%PRIVATE LABEL%';
QUIT;


DATA pea.Peanut_Prod_2 (KEEP = L5 UPC VOL_EQ);
SET pea.Peanut_Prod_2;
RUN;


/* -------- Joining the product &  panel and store data --------- */

PROC SQL;
CREATE TABLE pea.Peanut_Prod_Final AS
SELECT * FROM
(SELECT * FROM pea.Peanut_Prod_2) AS a 
INNER JOIN
(SELECT * FROM pea.Ps_Peanut) AS b
ON a.UPC=b.UPC;
QUIT;


/* ------ Data Processing ----------- */


/* Filtering for stores and weeks with sales of all 3 brands */

PROC SQL;
CREATE TABLE pea.Brands_3 AS
SELECT COUNT(DISTINCT(L5)) AS C_L5,IRI_KEY,WEEK FROM pea.Peanut_Prod_Final 
GROUP BY IRI_KEY,WEEK HAVING C_L5 = 3;
QUIT;

PROC SQL;
CREATE TABLE pea.Peanut_Prod_Final_1 AS
SELECT * FROM
(SELECT * FROM pea.Brands_3) AS a
INNER JOIN
(SELECT * FROM pea.Peanut_Prod_Final ) AS b
ON a.WEEK=b.WEEK AND a.IRI_KEY = b.IRI_KEY;
QUIT;

DATA pea.Peanut_Prod_Final_1 (DROP = C_L5 P_UNITS P_DOLLARS);
SET pea.Peanut_Prod_Final_1;
RUN;


DATA pea.Peanut_Prod_Final_1;
SET pea.Peanut_Prod_Final_1;
TOT_VOL = VOL_EQ*16*UNITS;
PRICE_PER_OZ = DOLLARS/TOT_VOL;
RUN;

DATA pea.Peanut_Prod_Final_1;
SET pea.Peanut_Prod_Final_1;

IF F='NONE' THEN F_1=0;
ELSE F_1=1;

IF D='0' THEN D_1=0;
ELSE D_1=1;

IF PR='0' THEN PR_1=0;
ELSE PR_1=1;

RUN;

/**************** JIF ******************/

PROC SQL;
CREATE TABLE pea.Jif AS
SELECT * FROM pea.Peanut_Prod_Final_1 WHERE L5 LIKE '%JIF%';
QUIT;

PROC SQL;
CREATE TABLE pea.Jif_Grouped AS
SELECT IRI_KEY,WEEK,COLUPC,SUM(UNITS) AS TOT_UNITS, SUM(DOLLARS) AS TOT_DOLLARS, SUM(F_1) AS TOT_F, 
SUM(D_1) AS TOT_D, SUM(PR_1) AS TOT_PR, AVG(PRICE_PER_OZ) AS AVG_PPZ, SUM(TOT_VOL) AS TOT_OZ
FROM pea.Jif
GROUP BY IRI_KEY,WEEK,COLUPC;
QUIT;

PROC SQL;
CREATE TABLE pea.Jif_Weekly AS
SELECT IRI_KEY,WEEK,SUM(UNITS) AS TOT_IRI_WEEK_UNITS, SUM(TOT_VOL) AS TOT_IRI_WEEK_OZ
FROM pea.Jif
GROUP BY IRI_KEY,WEEK;
QUIT;

PROC SQL;
CREATE TABLE pea.Jif_Grouped_1 AS
SELECT A.*, B.* FROM 
(SELECT * FROM pea.Jif_Grouped) AS a
LEFT JOIN
(SELECT * FROM pea.Jif_Weekly) AS b
ON a.IRI_KEY=b.IRI_KEY AND a.WEEK=b.WEEK;
QUIT;

DATA pea.Jif_Grouped_1;
SET pea.Jif_Grouped_1;

W_PRICE = AVG_PPZ*(TOT_OZ/TOT_IRI_WEEK_OZ);

IF TOT_F=0 THEN W_FEATURE=0;
ELSE W_FEATURE=1*(TOT_OZ/TOT_IRI_WEEK_OZ);

IF TOT_D=0 THEN W_DISPLAY=0;
ELSE W_DISPLAY=1*(TOT_OZ/TOT_IRI_WEEK_OZ);

IF TOT_PR=0 THEN W_PROMOTION=0;
ELSE W_PROMOTION=1*(TOT_OZ/TOT_IRI_WEEK_OZ);

RUN;

PROC SQL;
CREATE TABLE pea.Jif_Final AS
SELECT IRI_KEY,WEEK,SUM(TOT_UNITS) AS KC_TOT_UNITS, SUM(TOT_DOLLARS) AS KC_TOT_DOLLARS,SUM(W_PRICE) AS KC_W_PRICE,
SUM(W_FEATURE) AS KC_W_FEATURE, SUM(W_DISPLAY) AS KC_W_DISPLAY, SUM(W_PROMOTION) AS KC_W_PROMOTION, SUM(TOT_OZ) AS KC_SUM_OZ
FROM pea.Jif_Grouped_1 
GROUP BY IRI_KEY,WEEK;
QUIT;

/**************** PRIVATE LABEL ******************/

PROC SQL;
CREATE TABLE pea.Pl AS
SELECT * FROM pea.Peanut_Prod_Final_1 WHERE L5 LIKE '%PRIVATE LABEL%';
QUIT;

PROC SQL;
CREATE TABLE pea.Pl_Grouped AS
SELECT IRI_KEY,WEEK,COLUPC,SUM(UNITS) AS TOT_UNITS, SUM(DOLLARS) AS TOT_DOLLARS, SUM(F_1) AS TOT_F, 
SUM(D_1) AS TOT_D, SUM(PR_1) AS TOT_PR, AVG(PRICE_PER_OZ) AS AVG_PPZ, SUM(TOT_VOL) AS TOT_OZ
FROM pea.Pl
GROUP BY IRI_KEY,WEEK,COLUPC;
QUIT;

PROC SQL;
CREATE TABLE pea.Pl_Weekly AS
SELECT IRI_KEY,WEEK,SUM(UNITS) AS TOT_IRI_WEEK_UNITS, SUM(TOT_VOL) AS TOT_IRI_WEEK_OZ
FROM pea.Pl
GROUP BY IRI_KEY,WEEK;
QUIT;

PROC SQL;
CREATE TABLE pea.Pl_Grouped_1 AS
SELECT A.*, B.* FROM 
(SELECT * FROM pea.Pl_Grouped) AS a
LEFT JOIN
(SELECT * FROM pea.Pl_Weekly) AS b
ON a.IRI_KEY=b.IRI_KEY AND a.WEEK=b.WEEK;
QUIT;

DATA pea.Pl_Grouped_1;
SET pea.Pl_Grouped_1;

W_PRICE = AVG_PPZ*(TOT_OZ/TOT_IRI_WEEK_OZ);

IF TOT_F=0 THEN W_FEATURE=0;
ELSE W_FEATURE=1*(TOT_OZ/TOT_IRI_WEEK_OZ);

IF TOT_D=0 THEN W_DISPLAY=0;
ELSE W_DISPLAY=1*(TOT_OZ/TOT_IRI_WEEK_OZ);

IF TOT_PR=0 THEN W_PROMOTION=0;
ELSE W_PROMOTION=1*(TOT_OZ/TOT_IRI_WEEK_OZ);

RUN;

PROC SQL;
CREATE TABLE pea.Pl_Final AS
SELECT IRI_KEY,WEEK,SUM(TOT_UNITS) AS KC_TOT_UNITS, SUM(TOT_DOLLARS) AS KC_TOT_DOLLARS,SUM(W_PRICE) AS KC_W_PRICE,
SUM(W_FEATURE) AS KC_W_FEATURE, SUM(W_DISPLAY) AS KC_W_DISPLAY, SUM(W_PROMOTION) AS KC_W_PROMOTION, SUM(TOT_OZ) AS KC_SUM_OZ
FROM pea.Pl_Grouped_1 
GROUP BY IRI_KEY,WEEK;
QUIT;


/**************** SKIPPY ******************/

PROC SQL;
CREATE TABLE pea.Skippy AS
SELECT * FROM pea.Peanut_Prod_Final_1 WHERE L5 LIKE '%SKIPPY%';
QUIT;

PROC SQL;
CREATE TABLE pea.Skippy_Grouped AS
SELECT IRI_KEY,WEEK,COLUPC,SUM(UNITS) AS TOT_UNITS, SUM(DOLLARS) AS TOT_DOLLARS, SUM(F_1) AS TOT_F, 
SUM(D_1) AS TOT_D, SUM(PR_1) AS TOT_PR, AVG(PRICE_PER_OZ) AS AVG_PPZ, SUM(TOT_VOL) AS TOT_OZ
FROM pea.Skippy
GROUP BY IRI_KEY,WEEK,COLUPC;
QUIT;

PROC SQL;
CREATE TABLE pea.Skippy_Weekly AS
SELECT IRI_KEY,WEEK,SUM(UNITS) AS TOT_IRI_WEEK_UNITS, SUM(TOT_VOL) AS TOT_IRI_WEEK_OZ
FROM pea.Skippy
GROUP BY IRI_KEY,WEEK;
QUIT;

PROC SQL;
CREATE TABLE pea.Skippy_Grouped_1 AS
SELECT A.*, B.* FROM 
(SELECT * FROM pea.Skippy_Grouped) AS a
LEFT JOIN
(SELECT * FROM pea.Skippy_Weekly) AS b
ON a.IRI_KEY=b.IRI_KEY AND a.WEEK=b.WEEK;
QUIT;

DATA pea.Skippy_Grouped_1;
SET pea.Skippy_Grouped_1;

W_PRICE = AVG_PPZ*(TOT_OZ/TOT_IRI_WEEK_OZ);

IF TOT_F=0 THEN W_FEATURE=0;
ELSE W_FEATURE=1*(TOT_OZ/TOT_IRI_WEEK_OZ);

IF TOT_D=0 THEN W_DISPLAY=0;
ELSE W_DISPLAY=1*(TOT_OZ/TOT_IRI_WEEK_OZ);

IF TOT_PR=0 THEN W_PROMOTION=0;
ELSE W_PROMOTION=1*(TOT_OZ/TOT_IRI_WEEK_OZ);

RUN;

PROC SQL;
CREATE TABLE pea.Skippy_Final AS
SELECT IRI_KEY,WEEK,SUM(TOT_UNITS) AS KC_TOT_UNITS, SUM(TOT_DOLLARS) AS KC_TOT_DOLLARS,SUM(W_PRICE) AS KC_W_PRICE,
SUM(W_FEATURE) AS KC_W_FEATURE, SUM(W_DISPLAY) AS KC_W_DISPLAY, SUM(W_PROMOTION) AS KC_W_PROMOTION, SUM(TOT_OZ) AS KC_SUM_OZ
FROM pea.Skippy_Grouped_1 
GROUP BY IRI_KEY,WEEK;
QUIT;


/* --------------- JOIN -------------------*/


DATA pea.Peanut_Prod_Final_1 (KEEP = PANID IRI_KEY WEEK L5 UPC);
SET pea.Peanut_Prod_Final_1;
RUN;

PROC PRINT DATA=pea.Peanut_Prod_Final_1(OBS=20);
RUN;

PROC SQL;
CREATE TABLE pea.Final_Jif AS
SELECT * FROM 
(SELECT * FROM pea.Peanut_Prod_Final_1) AS a
INNER JOIN
(SELECT * FROM pea.Jif_Final) AS b
ON a.IRI_KEY=b.IRI_KEY AND a.WEEK=b.WEEK;
QUIT;

PROC SQL;
CREATE TABLE pea.Final_Jif_Pl AS
SELECT * FROM 
(SELECT * FROM pea.Final_Jif) AS a
INNER JOIN
(SELECT * FROM pea.Pl_Final) AS b
ON a.IRI_KEY=b.IRI_KEY AND a.WEEK=b.WEEK;
QUIT;

PROC SQL;
CREATE TABLE pea.Final_Jif_Pl_Skippy AS
SELECT * FROM 
(SELECT * FROM pea.Final_Jif_Pl) AS a
INNER JOIN
(SELECT * FROM pea.Skippy_Final) AS b
ON a.IRI_KEY=b.IRI_KEY AND a.WEEK=b.WEEK;
QUIT;

PROC PRINT DATA=pea.Final_Jif_Pl_Skippy(OBS=20);
RUN;

/* -------------- Recreating Demographic Data -----------------*/

DATA pea.demo_1;
SET pea.demo;
RUN;


PROC SQL;
CREATE TABLE pea.Final_With_Demo AS
SELECT * FROM
(SELECT * FROM pea.Final_Jif_Pl_Skippy) AS a
LEFT JOIN
(SELECT * FROM pea.demo_1) AS b
ON a.PANID=b.PANELIST_ID;
QUIT;



/*************FINAL DATASET***************************/

DATA pea.Mdc_Final_With_Demo(DROP = Panelist_ID);
SET pea.Final_With_Demo ;
RUN;


/* --------------  MDC DATA ---------------- */


DATA pea.Mdc_Final_With_Demo (DROP = Panelist_Type	HH_RACE	Type_of_Residential_Possession	COUNTY	HH_AGE	HH_EDU	HH_OCC	Education_Level_Reached_by_Male	
Occupation_Code_of_Male_HH	Male_Working_Hour_Code	MALE_SMOKE	Education_Level_Reached_by_Femal	
Occupation_Code_of_Female_HH	Female_Working_Hour_Code	FEM_SMOKE	Children_Group_Code	Marital_Status	Language	
Number_of_TVs_Used_by_HH	Number_of_TVs_Hooked_to_Cable	Year	HISP_FLAG	HISP_CAT	HH_Head_Race__RACE2_	HH_Head_Race__RACE3_	
Microwave_Owned_by_HH	ZIPCODE	FIPSCODE	market_based_upon_zipcode	IRI_Geography_Number	EXT_FACT);
SET pea.Mdc_Final_With_Demo;
RUN;


PROC SQL;
CREATE TABLE pea.Mdc_Final_With_Demo AS
SELECT * FROM pea.Mdc_Final_With_Demo WHERE Combined_Pre_Tax_Income_of_HH <> 0;
QUIT;

PROC SQL;
CREATE TABLE pea.Mdc_Final_With_Demo AS
SELECT * FROM pea.Mdc_Final_With_Demo WHERE Age_Group_Applied_to_Female_HH <> 0;
QUIT;

PROC SQL;
CREATE TABLE pea.Mdc_Final_With_Demo AS
SELECT * FROM pea.Mdc_Final_With_Demo WHERE Age_Group_Applied_to_Male_HH <> 0;
QUIT;


DATA pea.Mdc_Final_With_Demo;
SET pea.Mdc_Final_With_Demo (RENAME = (Combined_Pre_Tax_Income_of_HH = INCOME Family_Size = FAM_SIZE Age_Group_Applied_to_Female_HH = FEMALE_AGEGP
Age_Group_Applied_to_Male_HH = MALE_AGEGP));
NUM_PETS = Number_of_Dogs + Number_of_Cats;
RUN;

data pea.Mdc_Final_With_Demo;
set pea.Mdc_Final_With_Demo;
if nmiss(of _numeric_)  > 0 then delete;
run;


DATA pea.Mdc_Final_With_Demo (DROP = Number_of_Dogs Number_of_Cats);
SET pea.Mdc_Final_With_Demo;
RUN;


DATA pea.Mdc_Final_With_Demo;
SET pea.Mdc_Final_With_Demo;
IF FIND(L5,'JIF','i') GE 1 THEN BC=1;
IF FIND(L5,'PRIVATE LABEL','i') GE 1 THEN BC=2;
IF FIND(L5,'SKIPPY','i') GE 1 THEN BC=3;
RUN;


/* ------- DATA PREP FOR MDC --------- */

/* create a format to group missing and nonmissing */
proc format;
 value $missfmt ' '='Missing' other='Not Missing';
 value  missfmt  . ='Missing' other='Not Missing';
run;
 
/** LOOKING FOR MISSING DATA */

proc freq data=pea.Mdc_Final_With_Demo; 
format _NUMERIC_ missfmt.;
tables _NUMERIC_ / missing missprint nocum nopercent;
run;
data pea.Mdc_Final_With_Demo;
set pea.Mdc_Final_With_Demo;
if nmiss(of _numeric_)  > 0 then delete;
run;

DATA pea.Mdc_Final(KEEP=ID  DECISION MODE PRICE PR F D INCOME FAM_SIZE FEMALE_AGEGP NUM_PETS MALE_AGEGP);
SET pea.Mdc_Final_With_Demo;
ARRAY PR_A{3} KC_W_PROMOTION PG_W_PROMOTION PL_W_PROMOTION;
ARRAY D_A{3} KC_W_DISPLAY PG_W_DISPLAY PL_W_DISPLAY;
ARRAY PRICE_A{3} KC_W_PRICE PG_W_PRICE PL_W_PRICE;
ARRAY F_A{3} KC_W_FEATURE PG_W_FEATURE PL_W_FEATURE;
RETAIN ID 0;
ID+1;
DO I=1 TO 3;
MODE=I;
PR=PR_A{I};
D=D_A{I};
PRICE=PRICE_A{I};
F=F_A{I};
DECISION=(BC=I);
OUTPUT;
END;
RUN;


/* -------- ADD DEMO DATA FAMILY SIZE, PETS, FEMALE AGE AND INCOME LEVEL -------- */

DATA pea.Mdc_Final;
SET pea.Mdc_Final;
BC1=0;
BC2=0;
BC3=0;
IF MODE = 1 THEN BC1 = 1;
IF MODE = 2 THEN BC2 = 1;
IF MODE = 3 THEN BC3 = 1;

FAM1 = FAM_SIZE*BC1;
FAM2 = FAM_SIZE*BC2;
FAM3 = FAM_SIZE*BC3;

INC1 = INCOME*BC1;
INC2 = INCOME*BC2;
INC3 = INCOME*BC3;

FAGEGP1 = FEMALE_AGEGP*BC1;
FAGEGP2 = FEMALE_AGEGP*BC2;
FAGEGP3 = FEMALE_AGEGP*BC3;

MAGEGP1 = MALE_AGEGP*BC1;
MAGEGP2 = MALE_AGEGP*BC2;
MAGEGP3 = MALE_AGEGP*BC3;

NPETS1 = NUM_PETS*BC1;
NPETS2 = NUM_PETS*BC2;
NPETS3 = NUM_PETS*BC3;

RUN;

/* create a format to group missing and nonmissing */
proc format;
 value $missfmt ' '='Missing' other='Not Missing';
 value  missfmt  . ='Missing' other='Not Missing';
run;
 
proc freq data=pea.Mdc_Final; 
format _NUMERIC_ missfmt.;
tables _NUMERIC_ / missing missprint nocum nopercent;
run;


PROC MEANS DATA = pea.Mdc_Final; 
VAR PRICE;
run;
/*Imputing Missing Data */
Data pea.Mdc_Final;
set  pea.Mdc_Final;
IF PR = '.' then PR = 0;
IF D = '.' then D = 0;
IF F = '.' then F  = 0;
IF PRICE = '.' then PRICE = 0.1089988;
run;
PROC MDC DATA=pea.Mdc_Final;
MODEL DECISION= PRICE BC1 BC2 BC3  PR D F FAM1 FAM2 FAM3 INC1 INC2 INC3 FAGEGP1 FAGEGP2 FAGEGP3  MAGEGP1 MAGEGP2 MAGEGP3  NPETS1 NPETS2  NPETS3 /TYPE=CLOGIT NCHOICE=3;
ID ID;
OUTPUT OUT=pea.Prob_Mdc PRED=P;
RUN;


PROC MEANS DATA = pea.Mdc_Final; 
VAR PRICE; 
CLASS MODE;
RUN;


/* ------------ PREDICTION -------------- */

PROC SQL;
CREATE TABLE pea.Predict AS 
SELECT P, ID, DECISION
FROM pea.Prob_Mdc
ORDER BY ID, P DESC;
QUIT;

DATA pea.Predict;
SET pea.Predict; 
PREDICT=0;
BY ID;
IF FIRST.ID THEN PREDICT=1;
RUN;

PROC FREQ DATA=pea.Predict;
TABLE PREDICT*DECISION;
RUN;


