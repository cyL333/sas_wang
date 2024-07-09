proc datasets library=work kill nolist;run;
quit;

libname project "C:\Users\lcyhk\Desktop\exercise";

proc import datafile="C:\Users\lcyhk\Desktop\exercise\LINKTABLE.csv"
            out=project.LINKTABLE
            dbms=csv
            replace;
    getnames=yes; /* 假设 Excel 文件的首行为列名 */
run;

*==========================================================================================
Bank-borrower lending relationships
===========================================================================================;
proc sql;
   create table LINKTABLE
   as select GVKEY,bcoid,coname_h,facid as FACILITYID,facstartdate,FIC
   from  project.LINKTABLE
   order by GVKEY, bcoid desc;
   quit; 

data LINKTABLE; set LINKTABLE; format fic $5.; year= year(facstartdate); run;
proc sort data = LINKTABLE nodupkey;by FACILITYID gvkey;run; * 0 dup;

** Compustat firms;
proc sql noprint;
    create table ccm_loc 
	as select unique
	input(gvkey,6.) as gvkey,cusip,substr(cusip,1,8) as cusip8,cik,year(datadate) as year,loc
    from project.funda1970_2018	
	where 1987<=fyear<=2018
	and datadate>='01JAN1975'd
	and indfmt ='INDL' and datafmt = 'STD' and popsrc = 'D' and consol = 'C'
    order by gvkey,year;
	quit; 

proc sql noprint;
	create table LINKTABLE
	as select *
	from LINKTABLE as a left join ccm_loc as b
	on a.gvkey = b.gvkey
	and a.year = b.year
	order a.gvkey,a.year;
	quit;

data LINKTABLE;	set LINKTABLE; if LOC ~= "";if LOC ="USA" | fic = "USA"; run; 

** Get facility information; 
data facility;
	set project.tfn_facility;
	where countryofsyndication = "USA";
	run;

proc sql noprint;
	create table ds_facility
	as select a.*, year(b.FacilityStartDate) as cyear,year(b.FacilityEndDate) as endyear,b.PACKAGEID, 
        b.FacilityEndDate,b.FacilityStartDate,b.maturity,(b.FACILITYAMT*b.ExchangeRate) as FACILITYAMT	
    from linktable as a left join facility as b
	on a.FACILITYID = b.FACILITYID
	where PACKAGEID ~=.
	order by a.gvkey,a.facstartdate;
	quit;

proc sort data = ds_facility nodupkey; by gvkey packageid facilityid;run; * 0 dup;
proc means data = ds_facility n mean std p25 p50 p75;
	var maturity FACILITYAMT;
run;


** Get package information;
proc sql;
    create table ds_full
    as select a.*,b.active,b.DealActiveDate,b.DealAmount
    from ds_facility as a join project.tfn_package as b
    on a.PACKAGEID = b.PACKAGEID
	and a.bcoid = b.BORROWERCOMPANYID
	order by a.gvkey,a.packageid,a.facilityid;
	quit;

proc sort data=ds_full nodupkey dupout=checkme;by gvkey facilityid; run; *0 dup;
proc sort data = ds_full out = firms nodupkey;by gvkey;run;

** Lender/bank information;
data lendershares;
	set project.tfn_lendershares;run;
proc sort data = lendershares; by facilityid companyid;run;

data lendershares;
     set  lendershares;
     if LeadArrangerCredit = "Yes";
	 if lenderrole= "Accepting bank" | lenderrole="Adviser" | lenderrole="Collateral agent"|
   		lenderrole="Dealer" | lenderrole= "Financial adviser" | lenderrole="Fronting bank" | 
   		lenderrole="Lead participant" | lenderrole="Lease arranger" | 
   		lenderrole= "Participant"     | lenderrole="Paying agent" | 
        lenderrole="Placement agent"  | lenderrole="Reference agent"  |
        lenderrole="Secondary investor" | lenderrole="Sub-underwriter"  |
	    lenderrole="Underwriter" then delete;
	 rename companyid = lenderid;
	 drop agentcredit LeadArrangerCredit BANKALLOCATION;
run;

** Lender's country; 
data company;
	set project.tfn_company;
	country = upcase(country);
	keep companyid company country InstitutionType PublicPrivate PRIMARYSICCODE;
run;

proc sql;
    create table lender_plus
    as select a.*,b.Country as bank_Country,b.company as bankname, int(PRIMARYSICCODE/1000) as sic1,
				b.InstitutionType,b.PublicPrivate
	          from lendershares as a join company as b
			  on a.lenderid = b.companyid
              order by bank_country,lender;
			  quit;

** Merge the lenders information to the loan;
proc sql noprint;
	create table ds_ful_bank
	as select a.gvkey,a.cusip,a.cusip8,a.cik,b.lenderid,a.cyear,a.endyear,a.coname_h,b.lender,b.bankname,a.*,b.*
	from ds_full as a join lender_plus as b
	on a.FACILITYID = b.FACILITYID
	order by a.gvkey,b.lenderid,a.facstartdate;
	quit; 

data ds_ful_bank; set ds_ful_bank;if InstitutionType ~="US Bank";if bank_country~="USA";run;

proc sort data = ds_ful_bank out = firm nodupkey; by gvkey;run; 
proc sort data = ds_ful_bank out = bank nodupkey; by lenderid;run;

** create bank-firm pairs;
data bank_firm_pair;
	set ds_ful_bank;
	keep gvkey cusip cusip8 cik lenderid cyear endyear;
	run;

proc sort data = bank_firm_pair nodupkey;
	by gvkey lenderid cyear endyear;
	run;

** lending relationship from first loan initiation to last loan's maturity;
proc means data = bank_firm_pair noprint;
	by gvkey lenderid;
	id cusip cusip8 cik;
	output out = bank_firm_pair2 (drop=_type_ _freq_)
		   min(cyear)   = byear
		   max(endyear) = eyear;
		   run;

data bank_firm_pair2;set bank_firm_pair2;if eyear ~=.;run; 

proc sort data = bank_firm_pair2 out = firm nodupkey; by gvkey;run; 
proc sort data = bank_firm_pair2 out = bank nodupkey; by lenderid;run; 

** Create bank-firm-panel;
data year;
	input year @@;
	cards;
    1987 1988 1989 1990 1991 1992 1993 1994 1995 1996 1997 1998 1999 2000
	2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014
	2015 2016 2017 2018 2019 2020 2021 2022 2023 2024 2025 2026 2027 2028
    2029 2030 2031
	;
	run;

proc sql noprint;
	create table bank_firm_panel 
			as select *
			from bank_firm_pair2 as a,year as b 
			where a.byear<=b.year<=a.eyear
			order by gvkey,lenderid,year;quit; 

** Merge back the lender-plus information;
proc sql noprint;
	create table bank_firm_panel
	as select a.gvkey,a.cusip,a.cusip8,a.cik,a.lenderid,a.year,b.Country as bank_Country,
			b.company as bankname,b.InstitutionType,b.PublicPrivate,a.byear,a.eyear
	from bank_firm_panel as a left join company as b
	on a.lenderid = b.companyid
	order by a.gvkey,a.lenderid,a.year;
	quit;

** save;
data project.bank_firm_panel;set bank_firm_panel; run;

**END;
