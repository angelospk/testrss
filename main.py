import atexit
import json
import os
from flask import Flask, jsonify, make_response, request
import requests
from apscheduler.schedulers.background import BackgroundScheduler
from threading import Thread
from datetime import datetime
from pathlib import Path
import subprocess
import pytz
import pandas as pd
from urllib.parse import unquote
import html
import logging
from tzlocal import get_localzone

logging.basicConfig(filename="log.txt",
                    level=logging.DEBUG,
                    format='%(asctime)s %(levelname)-8s %(message)s',
                    datefmt='%Y-%m-%d %H:%M:%S')
#timezone = get_localzone()
#print(timezone)
timezone = pytz.timezone("Europe/Athens")
app = Flask(__name__)
app.json.ensure_ascii = True

# variable to store the last time the function was executed
#last_time = datetime.now(timezone)

# columns = ["title", "info", "details", "site", "timestamp", "online"]
# off = pd.DataFrame(columns=columns)
off = pd.read_pickle("backup.pkl")


def save_backup():
  off.to_pickle("backup.pkl")
  with open("sub_list.json", "w") as g:
    json.dump(sub_list, g)
  # logging()
  logging.debug("Dataframe backed up!")


atexit.register(save_backup)
#logging.basicConfig(filename="log.txt", level=logging.DEBUG, format='%(asctime)s %(levelname)-8s %(message)s', datefmt='%Y-%m-%d %H:%M:%S')
with open("sub_list.json", "r") as g:
  sub_list = json.load(g)


@app.route("/offers/rss/search/<keyword>")
def search_offers(keyword):
  global off
  keyword = unquote(keyword)

  matches = off[(off['title'].str.contains(keyword, case=False, regex=True)) |
                (off['details'].str.contains(keyword, case=False, regex=True))]
  logging.debug(f"vrethikan gia to {keyword} {len(matches)} apotelesmata")
  # return jsonify(matches.to_dict(orient='records'))
  return create_rss(matches)


@app.route("/offers/json/search/<keyword>")
def search_json_offers(keyword):
  global off
  keyword = unquote(keyword)
  #print(keyword, len(off))
  matches = off[
    (off['title'].str.contains(keyword, case=False, regex=True)) |
    (off['details'].str.contains(keyword, case=False, regex=True))].iloc[:25]
  # print(len(matches))
  # return jsonify(matches.to_dict(orient='records'))
  return create_json(matches)


@app.route("/offers/rss/site/<keyword>")
def site_rss_offers(keyword):
  global off
  keyword = unquote(keyword)
  # print(keyword, len(off))
  matches = off[off['site'].str.contains(keyword, case=False, regex=True)]
  # ret = matches[["url", "details"]]
  # ret["details"] = ret["details"].apply(len)
  ret = matches.drop(columns=["details"])
  # print(len(matches))
  return create_rss(ret)


@app.route("/offers/json/site/<keyword>")
def site_old_offers(keyword):
  global off
  keyword = unquote(keyword)
  # print(keyword, len(off))
  matches = off[off['site'].str.contains(keyword, case=False, regex=True)]
  # ret = matches[["url", "details"]]
  # ret["details"] = ret["details"].apply(len)
  # ret = matches.drop(columns=["timestamp", "details", "site"])
  # print(len(matches))
  return create_json(matches)


@app.route("/offers/json")
def create_json(df=None):
  global off
  if (type(df) == type(None)):
    df = off
  active_df = df.loc[df['online'] == True]
  active_df = active_df.drop(columns=["timestamp", "details", "online"])
  active_dic = dict(
    zip(active_df.index.values.tolist(), active_df.values.tolist()))
  return json.dumps(active_dic, ensure_ascii=False).encode('utf8')


@app.route("/offers/rss")
def create_rss(df=None):
  # Create the RSS feed
  global off
  if (type(df) == type(None)):
    df = off
  active_df = df.loc[df['online'] == True].iloc[:25]
  # print(active_df)
  # print(df)
  rss = """<?xml version="1.0" encoding="UTF-8"?>
    <rss version="2.0">
    <channel>
    <title>My RSS Feed</title>
    <link>https://mywebsite.com</link>
    <description>My RSS Feed Description</description>"""
  # Iterate through the rows of the DataFrame
  for _, row in active_df.iterrows():
    dt_object = datetime.fromtimestamp(row["timestamp"])
    # Format the datetime object to the RSS-formatted date
    rss_date = dt_object.strftime('%a, %d %b %Y %H:%M:%S +0000')
    rss += """
        <item>
        <title>{}</title>
        <guid isPermaLink="true">{}</guid>
        <link>{}</link>
         <pubDate>{}</pubDate>
        <description>{}</description>
        </item>""".format(html.escape(row["title"]), _, _, rss_date,
                          html.escape(row["info"]))
  rss += """
    </channel>
    </rss>"""
  response = make_response(rss)
  response.content_type = "application/rss+xml"
  response.headers.add('Content-Type', 'application/rss+xml; charset=utf-8')
  return response


@app.route("/log<num>")
def get_logs(num):
  # Set the file path
  file_path = "log.txt"

  # Use the `tail` command to output the last 10 lines of the file
  output = subprocess.run(["tail", "-n", str(num), file_path],
                          capture_output=True)

  # Print the output
  return output.stdout.decode("utf-8").replace('\n', '<br>')


@app.route("/ls")
def get_ls():
  # execute the bash command and get the output as bytes
  command = "ls . -lt"
  process = subprocess.run(command,
                           shell=True,
                           stdout=subprocess.PIPE,
                           stderr=subprocess.PIPE)
  output = process.stdout.decode('utf-8').replace('\n', '<br>')
  # return the stdout attribute of the CompletedProcess instance as a string
  return output


def clean_off():
  global off
  old_off = off.loc[off["online"] == False
                    & off.timestamp.apply(OlderThan13Days)]
  off = off.drop(old_off.index.values.tolist(), axis="index")
  # offline["cond"]=offline["timestamp"].apply(OlderThan13Days)
  # old_off=off.loc[off["online"]==False and abs(datetime.fromtimestamp(off["timestamp"]).day-t.day)>13]


def run_task():
  global last_time
  # get the current time
  now = datetime.now(timezone)
  # print the current time
  print(f"The task was executed at {now}")
  # update the last_time variable with the current time
  last_time = now


@app.route("/")
def home():
  # mainapp()
  return ("eisai spiti")


@app.route("/subs/new", methods=["POST"])
def add_to_list():
  global sub_list
  data = request.get_json()
  # print(data)
  urls = {d['url'] for d in sub_list}
  lista = data["lista"]
  lista.reverse()
  for item in lista:
    if item['url'] not in urls:
      item["timestamp"] = datetime.now(timezone).timestamp()
      sub_list.append(item)
  sub_list.sort(key=lambda x: x["timestamp"], reverse=True)
  with open("sub_list.json", "w") as g:
    json.dump(sub_list, g)
  return jsonify({"message": "Data added to list"})


@app.route("/offers/new/<site>", methods=["POST"])
def add_offers(site):
  global off
  data = request.get_json()
  # print(data)
  # update
  #updates = data["updates"]
  m = off[off["site"] == site]
  # upd = dict(zip(list(m.index.values), updates))
  # for key in upd:
  #   m.loc[key, upd[key].keys()] = upd[key].values()
  # off.update(m)
  # new
  lista = data["lista"]
  with open("lista.json", "w") as g:
    json.dump(lista, g)
  temp = pd.DataFrame(lista)
  temp.url = temp.url.apply(unquote)
  temp.set_index("url", inplace=True)
  temp = temp.drop_duplicates(subset=["title"])
  off.update(temp)
  if (site in ["e-shop", "insomnia"]):
    old = m[~m.index.isin(temp.index)]
    old.loc[:, "online"] = False
    #logging.debug(old.online.values.tolist())
    off.update(old)
    #logging.debug(old)
  # temp["timestamp"]=int(datetime.now(timezone).timestamp())
  # temp["site"]=site
  new_data = temp[~temp.index.isin(m.index)]
  logging.debug(new_data)
  #print(f"New found: {len(new_data)}")
  tmst = int(datetime.now(timezone).timestamp())
  new_data = new_data.assign(timestamp=tmst, site=site)
  # new_data=new_data.insert()
  # new_data.insert("timestamp")
  # new_data["site"] = site
  #off = off.astype({"online": bool})
  new_data.astype({"online": bool})
  off = pd.concat([off, new_data])
  # urls = set(off["url"])
  # for item in lista:
  #   if item["url"] not in urls:
  #     item["timestamp"] = int(datetime.now(timezone).timestamp())
  #     item["site"] = site
  #     #off = off.assign(online=item["online"])
  #     next_index = len(off)
  #     off.loc[next_index] = item
  off = off.sort_values(by="timestamp", ascending=False)
  if (len(new_data) > 0):
    backup()
  # print(len(off))
  return jsonify({"message": "Data added to dataframe"})


@app.route("/subs/rss/<lang>", methods=["GET"])
def return_rss_by_language(lang):
  filtered_list = [item for item in sub_list if item["lang"] == lang]
  rss = '<?xml version="1.0"?>'
  rss += '<rss version="2.0">'
  rss += '<channel>'
  rss += '<title>Subtitles RSS Feed</title>'
  rss += '<link>https://subscene.com/</link>'
  rss += '<description>RSS feed for subtitles on Subscene</description>'

  for item in filtered_list:
    rss += '<item>'
    rss += '<title>' + item['title'] + '</title>'
    rss += '<link>' + item['url'] + '</link>'
    rss += '<description>Language: ' + item['lang'] + ' - Year: ' + item[
      'year'] + " - Type: " + item['type'] + '</description>'
    rss += '</item>'

  rss += '</channel>'
  rss += '</rss>'

  return rss, 200, {'Content-Type': 'application/rss+xml'}


@app.route("/backup")
def backup():
  save_backup()
  return "data backup complete!"


def OlderThan13Days(timest):
  now = datetime.today(timezone).day
  a = abs(datetime.fromtimestamp(timest).day - now)
  return a > 13


@app.route("/subs/rss", methods=["GET"])
def return_rss():
  global sub_list
  rss = '<?xml version="1.0"?>'
  rss += '<rss version="2.0">'
  rss += '<channel>'
  rss += '<title>Subtitles RSS Feed</title>'
  rss += '<link>https://subscene.com/</link>'
  rss += '<description>RSS feed for subtitles on Subscene</description>'

  for item in sub_list:
    rss += '<item>'
    rss += '<title>' + item['title'] + '</title>'
    rss += '<link>' + item['url'] + '</link>'
    rss += '<description>Language: ' + item['lang'] + ' - Year: ' + item[
      'year'] + " - Type: " + item['type'] + '</description>'
    rss += '</item>'

  rss += '</channel>'
  rss += '</rss>'

  return rss, 200, {'Content-Type': 'application/rss+xml'}


@app.route('/post_json', methods=['POST'])
def process_json():
  data = json.loads(request.data)
  return data


def run():
  try:
    app.run(host='0.0.0.0', port=8080)
  except Exception as e:
    save_backup()
    raise e


def keep_alive():
  t = Thread(target=run)
  t.start()


if __name__ == "__main__":
  # create a new scheduler to run the task every hour
  scheduler = BackgroundScheduler()
  scheduler.add_job(clean_off, "interval", days=14)
  scheduler.start()

  keep_alive()
