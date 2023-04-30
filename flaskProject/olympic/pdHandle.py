import pandas
from sqlalchemy import create_engine
from olympic.conMysql import Connect
from olympic import conMysql
#第一题
print(pandas.read_sql("select Year, sum(Gold)+sum(Silver)+sum(Bronze) from olympic  group by Year",conMysql.get_createengine()))
#第二小题的第一题
print(pandas.read_sql(" select  Country,Gold+Silver+Bronze as medal "
                      "from olympic where Year=2008 ORDER BY medal DESC LIMIT 10",conMysql.get_createengine()))
#第二 二题
print(pandas.read_sql("SELECT  Year,Country, medal/(SELECT sum(Gold+Silver+Bronze) "
                      "FROM olympic WHERE YEAR=2008) as billie  "
                      "FROM (SELECT YEAR,Country,Gold+Silver+Bronze AS medal "
                      "FROM olympic WHERE YEAR=2008 ORDER BY medal DESC LIMIT 10) AS resule",conMysql.get_createengine()))


#第三题
print(pandas.read_sql(" select  Year ,Country,Gold+Silver+Bronze as medal "
                      "from olympic where   Country='CHN' order by Year DESC"  ,conMysql.get_createengine()))
#第四题
print(pandas.read_sql(" select Discipline,count(*) as coun from adc   where Country='CHN'  group by Discipline  ORDER BY coun desc limit 14",conMysql.get_createengine()))


#updata  EMPLOYEE set AGE WHERE NAME ='Mon'