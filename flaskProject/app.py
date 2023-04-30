import json

import pandas
from flask import Flask, render_template, make_response, request

from olympic import conMysql
from olympic import confInfo

app = Flask(__name__)


@app.route('/index', methods=["GET", "POST"])
def hello_world():  # put application's code here

    return render_template("index.html")

#历届奥运会中奖奖牌变化情况
@app.route('/bs2', methods=["POST"])
def bs2():  # put application's code here
    data = pandas.read_sql("select Year, sum(Gold)+sum(Silver)+sum(Bronze) as sum from {} group by Year".format(confInfo.TABLE),
                           conMysql.get_createengine())
    return make_response(json.dumps(dict(zip(data["Year"], data['sum']))))


#某年前十国家奖牌比率
@app.route('/bs4', methods=["POST"])
def bs4():  # put application's code here
    year = request.form.get("reYear")

    data = pandas.read_sql(" select  Country,Gold+Silver+Bronze as medal ,(SELECT sum(Gold+Silver+Bronze)"
                           "FROM {} WHERE YEAR={}) as billie "
                           "from {} where Year={} ORDER BY medal DESC LIMIT 10".format(confInfo.TABLE,year, confInfo.TABLE,year),
                           conMysql.get_createengine())
    data["billie"] = data["medal"] / data["billie"]
    return make_response(
        json.dumps(dict(zip(data["Country"], zip(data['medal'], data["billie"].map(lambda x: round(x * 100, 2)))))))


#我国奥运会项目获奖分布情况
@app.route('/bs1', methods=["POST"])
def bs1():  # put application's code here

    data = pandas.read_sql(
        " select Discipline,count(*) as coun from  {}   where Country='CHN'  group by Discipline  ORDER BY coun desc limit 14".format(confInfo.MYSQL_QU_TABLE),conMysql.get_createengine())

    return make_response(json.dumps(dict(zip(data['Discipline'], data["coun"]))))


#中国历届奥运会中奖奖牌变化
@app.route('/bs3', methods=["POST"])
def bs3():  # put application's code here
    data = pandas.read_sql(" select  Year ,Country,Gold+Silver+Bronze as medal "
                           "from {} where   Country='CHN' order by Year DESC".format(confInfo.TABLE), conMysql.get_createengine())
    return make_response(json.dumps(dict(zip(data['Year'], data["medal"]))))


if __name__ == '__main__':
    app.run()
