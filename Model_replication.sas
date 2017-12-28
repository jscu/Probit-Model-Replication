*Feel free to change this directory to where you put your data in;
libname mylib '/data';


******************** LOGTA ********************;
data final;	set mylib.funda;
	*If at is missing, set LOGTA to missing;
	if at = . then LOGTA = .;	
	
	*If at is less than or equal to 0, set LOGTA to 0;
	else if at <= 0 then LOGTA = 0;
	else LOGTA = log(at);
run;


******************** LOGSEG ********************;
data logseg; set mylib.segmerged;
	
	*Only keep records that have valid values for all three variables;
	if gvkey AND datadate AND sid;	
run;

*Only keep one record if the records share the same gvkey, datadate and sid;
proc sort data = logseg nodupkey out = logseg1; by gvkey datadate sid;

data logseg2; set logseg1; by gvkey datadate sid;
	
	*Iterate the number of segments;
	if first.datadate = 1 then count = 0;
	count  = count + 1;
	retain count;
run;

data logseg3; set logseg2; by gvkey datadate sid;
	
	*Only keep the last record of the year that has the complete segment count;
	if last.datadate;		
run;

proc sql;
	create table final1 as
	select a.*, log(b.count) as LOGSEG
	from final as a left join logseg3 as b
	on a.gvkey = b.gvkey and a.datadate = b.datadate;
quit;


******************** FOREIGN ********************;
proc sort data = final1; by gvkey datadate;

data final2; set final1; by gvkey datadate;
	
	*The firm has foreign operations if foreign currency adjustment is not missing;
	if FCA ~=. then FOREIGN = 1;	
	else FOREIGN = 0;								
run;


******************** M&A ********************;
data m_and_a; set mylib.fncd;
run;

*Sort the fncd file by having a non-missing sale_fn value first;
proc sort data = m_and_a; by gvkey datadate DESCENDING sale_fn;

*If there are more than one record share the same gvkey and datadate, only keep the record with a valid sale_fn value or the first record;
proc sort data = m_and_a nodupkey out = m_and_a1; by gvkey datadate;	

data m_and_a2; set m_and_a1; by gvkey datadate;
	
	*If sale_fn is "AA" or "AB" then the firm experiences a significant merger/acquisition;
	if sale_fn = "AA" OR sale_fn = "AB" then MA = "True";	
	else MA = "False"; 
run;

data m_and_a3; set m_and_a2; by gvkey datadate;
	last_1_gvkey = lag(gvkey);
	last_2_gvkey = lag2(gvkey);
	last_1_year = lag(year(datadate));
	last_2_year = lag2(year(datadate));
	last_1_MA = lag(MA);
	last_2_MA = lag2(MA);
	
	*If the firm experiences a significant merger/acquisition in current or previous two years, MandA equals 1. Otherwise, 0;
	if MA = "True" then MandA = 1;	
	else if last_1_MA = "True" AND last_1_gvkey = gvkey AND last_1_year = year(datadate) - 1 then MandA = 1;
	else if last_2_MA = "True" AND last_2_gvkey = gvkey AND last_2_year = year(datadate) - 2 then MandA = 1;
	else MandA = 0;
run;

proc sql;
	create table final3 as
	select a.*, b.MandA as MandA
	from final2 as a left join m_and_a3 as b
	on a.gvkey = b.gvkey and a.datadate = b.datadate;
quit;


******************** ABNROA ********************;
data ABNROA;
	set final3;

	*Keep records that have valid SICH value;
	if sich;
run;
		
proc sort data = ABNROA; by gvkey datadate;

*Keep the first firm if the firm shares the same SICH for all years;
proc sort data = ABNROA out = ABNROA1 nodupkey; by gvkey;

data ABNROA1; set ABNROA1;
	sic = sich;
	keep gvkey sic;
run;

proc sort data = ABNROA1; by gvkey;

proc sort data = final3; by gvkey;

data ABNROA2; merge final3 ABNROA1; by gvkey; 
run;

*Drop dupcliated records;
proc sort data = ABNROA2 nodupkey out = ABNROA3; by gvkey datadate;

data ABNROA3; set ABNROA3;

	*If the firm has missing SICH code, use the SICH code from first year;
	if sich =. then sich = sic;
	
	*Convert it to two-digit;
	sic_code = int(sich/100);
	
	YEAR = year(datadate);
	last_1_gvkey = lag(gvkey);
	last_1_year = lag(year(datadate));
	last_1_at = lag(at);

	
	*If the company does not share same gvkey or the year does not match with previous year or total asset is missing, set avg_total_ass and ROA to missing;
	if gvkey ~= last_1_gvkey  OR YEAR ~= last_1_year + 1 OR at = .
	then do;
	avg_total_ass = .;
	ROA = .;
	end;
	
	*If total assets for both years are 0, set avg_total_ass and ROA to 0;
	else if (at = 0 AND last_1_at = 0)
	then do;
	avg_total_ass = 0;
	ROA = 0;
	end;
	
	*If income before extraordinary items deflated is missing, set ROA to missing;
	else if IB = .
	then do;
	avg_total_ass = (at + last_1_at ) / 2;
	ROA = .;
	end;
	
	else do;
	avg_total_ass = (at + last_1_at ) / 2;
	ROA = IB/avg_total_ass;
	end;
	
	drop sic;
run;

proc sort data = ABNROA3 out = ABNROA4; by YEAR sic_code;

*Calculate the industry-adjusted ROA by calculating the group mean first and then subtract each value from it;
proc means data = ABNROA4 noprint;
by YEAR sic_code;
var ROA;
output out = ABNROA5 
mean=mean_roa;
run;

proc sql;
 create table final4 as 
 select a.*, (a.roa - b.mean_roa) as ABNROA
   from ABNROA4 as a left join ABNROA5 as b 
     on a.YEAR = b.YEAR and a.sic_code = b.sic_code;	
quit;


data final4; set final4;
drop last_1_gvkey last_1_year last_1_at;
run;


******************** CR ********************;
data CR; set final4; by YEAR sic_code;

	*Drop the records with missing value for sale;
	if sale~=.;
run;

*Calculate the total sales by group;
proc sql; 
	create table CR1 as  
	select CR.*, sum(sale) as total_sale
	from CR group by YEAR, sic_code
	order by YEAR, sic_code;
quit;

data CR2; set CR1;

	*Calculate the market share for each group;
	if sale = . OR total_sale = . OR total_sale = 0 then share = .;
	else share = sale/total_sale;
	if share~=.;
run;

*Sort the data from the largest market share to smallest;
proc sort data = CR2; by YEAR sic_code Descending share;

*Find the first four firms that have the largest market share;
data CR3; set CR2; by YEAR sic_code Descending share;
	if first.sic_code = 1 then count = 0;
	count = count + 1;
	retain count;

    if count<=4;
run;

*Calculate the total of the first four largest market share by group;
proc sql; 
	create table CR4 as  
	select CR3.*, sum(share) as CR
	from CR3 group by YEAR, sic_code
	order by YEAR, sic_code;
quit;

*Only keep the first market share in each group because the market share is already the sum, which is the same for firms that share the same sic code;
data CR5; set CR4; by YEAR sic_code;
	if first.sic_code = 1;
	Keep YEAR sic_code CR;
run;

*Merge this dataset with the original dataset;
proc sql; 
	create table final5 as 
 	select a.*, b.CR 
    from final4 as a left join CR5 as b 
    on a.YEAR = b.YEAR and a.sic_code = b.sic_code; 
quit;

proc sort data = final5; by gvkey datadate;


******************** LEV ********************;
data final6; set final5; by gvkey datadate;
	if dltt = . OR at = . then LEV = .;
	else if at = 0 then LEV = 0;
	else LEV = dltt/at;
run;


******************** CONOWN ********************;
data final7; set final6; by gvkey datadate;
	if CSHR = . OR CSHO = . then CONOWN = .;
	else if CSHO = 0 then CONOWN = 0;
	else CONOWN = 1 - (CSHR / CSHO);
run;


******************** EXTFIN ********************;
data final8; set final7; by gvkey datadate;
    last_1_gvkey = lag(gvkey);
	last_2_gvkey = lag2(gvkey);
	last_1_year = lag(year(datadate));
	last_2_year = lag2(year(datadate));
	last_1_FINCF = lag(fincf);
	last_2_FINCF = lag2(fincf);
	
	if gvkey ~= last_1_gvkey  OR YEAR ~= last_1_year + 1 then last_1_FINCF = .;
	if gvkey ~= last_2_gvkey  OR YEAR ~= last_2_year + 2 then last_2_FINCF = .;
	
	*If the firm has three-year sum of net equity financing and net debt financing and a non-missing average total assets, compute EXTFIN. Otherwise, set it to missing;
	if fincf ~= . AND last_1_FINCF ~= . AND last_2_FINCF ~= . AND avg_total_ass ~= . AND avg_total_ass ~= 0 then do; 
    sumfin = fincf + last_1_FINCF + last_2_FINCF;
    EXTFIN = sumfin / avg_total_ass;
    end;
    else do;
    EXTFIN = .;
    end;
run;

data final8; set final8;
drop last_1_FINCF last_2_FINCF sumfin;
run;


******************** ABNOPCYCLE ********************;
proc sort data = final8; by gvkey datadate;

data ABNOPCYCLE; set final8; by gvkey datadate;
	
	*Compute ending inventory and ending accounts receivable;
	invt1 = lag(invt);
	rect1 = lag(rect);
	
	*If they do not refer to the same firm, set it to missing;
	if gvkey ~= last_1_gvkey  OR YEAR ~= last_1_year + 1 then do;
	invt1 = .;
	rect1 = .;
	end;
	
	*Compute Operating Cycle if all the values are non-missing;
	if invt ~= . AND invt1 ~=. AND cogs ~= . AND cogs ~= 0 then do;
	Oper_Cycle = ((invt + invt1)/2)/(cogs/365);
	end;
	else do;
	Oper_Cycle = .;
	end;
	
	*Compute days to collect receivables if all the values are non-missing;
	if rect ~= . AND rect1 ~=. AND sale ~= . AND sale ~= 0 then do;
	Day_Recei = ((rect + rect1)/2)/(sale/365);
	end;
	else do;
	Day_Recei = .;
	end;
	
	*Compute non-industry-adjusted operating cycle if operating cycle and days to collect receivables are not missing;
	if Oper_Cycle ~= . AND Day_Recei ~= . then CYCLE = Oper_Cycle + Day_Recei;
	else  CYCLE = .;
run;

proc sort data = ABNOPCYCLE; by YEAR sic_code;

*Calculate the industry-adjusted operating cycle by calculating the group mean first and then subtract each value from it;
proc means data = ABNOPCYCLE noprint;
by YEAR sic_code;
var CYCLE;
output out = ABNOPCYCLE1
mean = mean_cycle;
run;

proc sql;
 create table final9 as 
 select a.*, (a.CYCLE - b.mean_cycle) as ABNOPCYCLE
   from ABNOPCYCLE as a left join ABNOPCYCLE1 as b 
     on a.YEAR = b.YEAR and a.sic_code = b.sic_code;	
quit;

data final9; set final9;
drop invt1 rect1 Oper_Cycle Day_Recei CYCLE;
run;


******************** EARNVOL ********************;
proc sort data = final9; by gvkey datadate;

data final10; set final9; by gvkey datadate;
	ROA1 = ROA;
	ROA2 = lag(ROA);
	ROA3 = lag2(ROA);
	ROA4 = lag3(ROA);
	ROA5 = lag4(ROA);
	
	last_2_gvkey = lag2(gvkey);
	last_2_year = lag2(year(datadate));
	last_3_gvkey = lag3(gvkey);
	last_3_year = lag3(year(datadate));
	last_4_gvkey = lag4(gvkey);
	last_4_year = lag4(year(datadate));
	
	*If they do not refer to the same firm, set it to missing;
	if gvkey ~= last_1_gvkey  OR YEAR ~= last_1_year + 1 then ROA2 = .;
	if gvkey ~= last_2_gvkey  OR YEAR ~= last_2_year + 2 then ROA3 = .;
	if gvkey ~= last_3_gvkey  OR YEAR ~= last_3_year + 3 then ROA4 = .;
	if gvkey ~= last_4_gvkey  OR YEAR ~= last_4_year + 4 then ROA5 = .;
	
	*Compute the standard deviation of ROA over the four previous years;
	EARNVOL = STD(of ROA2-ROA5);
run;


******************** BTM ********************;
data final11; set final10; by gvkey datadate;
	
	*Calculate market value of equity = share price * number of shares;
	if CSHO = . OR PRCC_F = . then equity = .;
	else equity = CSHO*PRCC_F;
	
	*Calculate book-to-market ratio;
	if equity = . OR equity = 0 then BTM = .;
	else BTM = CEQ/equity;
		
	drop sic_code YEAR avg_total_ass ROA ROA1 ROA2 ROA3 ROA4 ROA5 last_1_gvkey last_2_gvkey last_3_gvkey last_4_gvkey last_1_year last_2_year last_3_year last_4_year equity;
run;



