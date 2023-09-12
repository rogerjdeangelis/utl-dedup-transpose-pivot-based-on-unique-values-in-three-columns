# utl-dedup-transpose-pivot-based-on-unique-values-in-three-columns
    %let pgm=utl-dedup-transpose-pivot-based-on-unique-values-in-three-columns;

    Dedup transpose piv0t based on unique values in three columns

    Interesting transpose. Great question.

    Problem
       Use the suffix of the column name (age_V1) and its value  to select the appropriate visit row and score

     1.wps
     2 wps sql
     3 wps python sql
     4 wps proc r sql

    The solution provides a metod to merge datasets on row numbers.
    Merge without any common columns.

    github
    https://tinyurl.com/2zz65jsy
    https://github.com/rogerjdeangelis/utl-dedup-transpose-pivot-based-on-unique-values-in-three-columns

    https://stackoverflow.com/questions/77068316/tidy pivot a table based on 3 columns in r

    /*                   _
    (_)_ __  _ __  _   _| |_
    | | `_ \| `_ \| | | | __|
    | | | | | |_) | |_| | |_
    |_|_| |_| .__/ \__,_|\__|
            |_|
    */

    options validvarname=upcase;
    libname sd1 "d:/sd1";

    data sd1.want(drop=age_: );
     array ages age_v1 age_v2 age_v3;
       do idx=1 to dim(ages);
          set sd1.have point=idx;
          age=ages[idx];
          output;
       end;
       stop;
    cards4;
    1 63 65 67 M v1 8
    1 63 65 67 M v2 4
    1 63 65 67 M v3 1
    ;;;;
    run;quit;

    /**************************************************************************************************************************/
    /*                                             |                                     |                                    */
    /*                                             |                                     |                                    */
    /*                                             |                                     |                                    */
    /* INPUT Use the sufix(V#) to get the row      |RULES                                |      OUTPUT                        */
    /*                                             |                                     |                                    */
    /* GENDER VISIT ID  AGE_V1 AGE_V2 AGE_V3 SCORE |                                     | GENDER AGE VISIT ID SCORE          */
    /*                                             |                                     |                                    */
    /*   M     v1    1    63     65     67     8   |Suffix V1 'AGE_V1' select visit row 1|   M     63    v1  1   8            */
    /*   M     v2    1    63     65     67     4   |Suffix V2 'AGE_V2' select visit row 2|   M     65    v2  1   4            */
    /*   M     v3    1    63     65     67     1   |Suffix V3 'AGE_V2' select visit row 3|   M     67    v3  1   1            */
    /*                                             |                                     |                                    */
    /**************************************************************************************************************************/

    /*
    / | __      ___ __  ___
    | | \ \ /\ / / `_ \/ __|
    | |  \ V  V /| |_) \__ \
    |_|   \_/\_/ | .__/|___/
                 |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    data sd1.want(drop=age_: );
     array ages age_v1 age_v2 age_v3;
       do idx=1 to dim(ages);
          set sd1.have point=idx;
          age=ages[idx];
          output;
       end;
       stop;
    run;quit;
    proc print;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  he WPS System                                                                                                         */
    /*                                                                                                                        */
    /*  bs    GENDER    VISIT    ID    SCORE    AGE                                                                           */
    /*                                                                                                                        */
    /*  1       M        v1       1      8       63                                                                           */
    /*  2       M        v2       1      4       65                                                                           */
    /*  3       M        v3       1      1       67                                                                           */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*___                                     _
    |___ \    __      ___ __  ___   ___  __ _| |
      __) |   \ \ /\ / / `_ \/ __| / __|/ _` | |
     / __/ _   \ V  V /| |_) \__ \ \__ \ (_| | |
    |_____(_)   \_/\_/ | .__/|___/ |___/\__, |_|
                       |_|                 |_|
    */

    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('

    libname sd1 "d:/sd1";
    options validvarname=any;
    proc transpose data=sd1.have(obs=1) out=ages(drop=_name_ rename=col1=AGE);
       var age_:;
    run;quit;

    proc sql;
      create
         table want as
      select
         r.AGE
        ,l.GENDER
        ,l.VISIT
        ,l.ID
        ,l.score
      from
         (SELECT  *, MONOTONIC() as hav from sd1.have) as l
      inner join
         (SELECT  *, MONOTONIC() as rag from ages) as r
      on
         r.rag = l.hav
    ;quit;

    proc print data=want;

    ');

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /*  he WPS System                                                                                                         */
    /*                                                                                                                        */
    /*  bs    GENDER    VISIT    ID    SCORE    AGE                                                                           */
    /*                                                                                                                        */
    /*  1       M        v1       1      8       63                                                                           */
    /*  2       M        v2       1      4       65                                                                           */
    /*  3       M        v3       1      1       67                                                                           */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*____                                    _   _
    |___ /  __      ___ __  ___   _ __  _   _| |_| |__   ___  _ __
      |_ \  \ \ /\ / / `_ \/ __| | `_ \| | | | __| `_ \ / _ \| `_ \
     ___) |  \ V  V /| |_) \__ \ | |_) | |_| | |_| | | | (_) | | | |
    |____/    \_/\_/ | .__/|___/ | .__/ \__, |\__|_| |_|\___/|_| |_|
                     |_|         |_|    |___/
    */


    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x("
    libname sd1 'd:/sd1';
    %array(_ages,values=%utl_varlist(sd1.have,keep=age_:));
    %put &=_ages2;
    %put &=_agesn;
    proc python;
    export data=sd1.have python=have;
    submit;
    from os import path;
    import pandas as pd;
    import numpy as np;
    import pandas as pd;
    from pandasql import sqldf;
    mysql = lambda q: sqldf(q, globals());
    from pandasql import PandaSQL;
    pdsql = PandaSQL(persist=True);
    sqlite3conn = next(pdsql.conn.gen).connection.connection;
    sqlite3conn.enable_load_extension(True);
    sqlite3conn.load_extension('c:/temp/libsqlitefunctions.dll');
    mysql = lambda q: sqldf(q, globals());
    ages = pdsql('''
         %do_over(_ages,phrase=select ? as age from have,between=union)
    ''');
    want = pdsql('''
      select
         r.age
        ,l.GENDER
        ,l.VISIT
        ,l.ID
        ,l.score
      from
         (SELECT  *, ROW_NUMBER() over() as hav from have) as l
      inner join
         (SELECT  *, ROW_NUMBER() over() as rag from ages) as r
      on
         r.rag = l.hav

    ''');
    print(want);
    endsubmit;
    ");

    /**************************************************************************************************************************/
    /*                                                                                                                        */
    /* The PYTHON Procedure                                                                                                   */
    /*                                                Note I used the wps/sas code generator to transpose the ages            */
    /*     age GENDER VISIT   ID  SCORE                                                                                       */
    /* 0  63.0     M     v1  1.0    8.0               %do_over(_ages,phrase=select ? as age from have,between=union)          */
    /* 1  65.0     M     v2  1.0    4.0                                                                                       */
    /* 2  67.0     M     v3  1.0    1.0               select AGE_V1 as age from have union                                    */
    /*                                                select AGE_V2 as age from have union                                    */
    /*                                                select AGE_V3 as age from have                                          */
    /*  he WPS System                                                                                                         */
    /*                                                                                                                        */
    /*  bs    GENDER    VISIT    ID    SCORE    AGE                                                                           */
    /*                                                                                                                        */
    /*  1       M        v1       1      8       63                                                                           */
    /*  2       M        v2       1      4       65                                                                           */
    /*  3       M        v3       1      1       67                                                                           */
    /*                                                                                                                        */
    /**************************************************************************************************************************/

    /*  _
    | || |   __      ___ __  ___   _ __
    | || |_  \ \ /\ / / `_ \/ __| | `__|
    |__   _|  \ V  V /| |_) \__ \ | |
       |_|     \_/\_/ | .__/|___/ |_|
                      |_|
    */


    proc datasets lib=sd1 nolist nodetails;delete want; run;quit;

    %utl_submit_wps64x('
    libname sd1 "d:/sd1";
    proc r;
    export data=sd1.have r=have;
    submit;
    library(dplyr);
    library(sqldf);
    ageCol<-grep("AGE",colnames(have));
    ages<-as.data.frame(t(have[,ageCol][1,]));
    colnames(ages)<-"AGE";
    want <- sqldf("
      select
         r.age
        ,l.GENDER
        ,l.VISIT
        ,l.ID
        ,l.score
      from
         (SELECT  *, ROW_NUMBER() over() as hav from have) as l
      inner join
         (SELECT  *, ROW_NUMBER() over() as rag from ages) as r
      on
         r.rag = l.hav
      ");
    want;
    endsubmit;
    import data=sd1.want r=want;
    run;quit;
    proc print data=sd1.want;
    run;quit;
    ');

    /**************************************************************************************************************************/
    /*                                                  |                                                                     */
    /*  The WPS/R System                                |  RULES                                                              */
    /*                                                  |                                                                     */
    /*    AGE GENDER VISIT ID SCORE                     |                                                                     */
    /*  1  63      M    v1  1     8                     |  1. Create   AGE  dataframe                                         */
    /*  2  65      M    v2  1     4                     |                                                                     */
    /*  3  67      M    v3  1     1                     |               63                                                    */
    /*                                                  |               65                                                    */
    /*  WPS                                             |               67                                                    */
    /*                                                  |  2. Merge with have on row number                                   */
    /*  Obs    AGE    GENDER    VISIT    ID    SCORE    |                                                                     */
    /*                                                  |                                                                     */
    /*   1      63      M        v1       1      8      |                                                                     */
    /*   2      65      M        v2       1      4      |                                                                     */
    /*   3      67      M        v3       1      1      |                                                                     */
    /*                                                  |                                                                     */
    /**************************************************************************************************************************/

    /*              _
      ___ _ __   __| |
     / _ \ `_ \ / _` |
    |  __/ | | | (_| |
     \___|_| |_|\__,_|

    */
se the suffix of the column name (age_V1) and its value  to select the appropriate visit row and score
