import pika
import sqlite3
import pandas as pd
from sqlalchemy import create_engine
import xml.etree.ElementTree as et
engine = create_engine('sqlite:///foo.db')

connection = pika.BlockingConnection(pika.ConnectionParameters(host='localhost'))
channel = connection.channel()


channel.queue_declare(queue='hello')

def to_xml(df, filename=None, mode='w'):
    def row_to_xml(row):
        xml = ['<item>']
        for i, col_name in enumerate(row.index):
            xml.append('  <field name="{0}">{1}</field>'.format(col_name, row.iloc[i]))
        xml.append('</item>')
        return '\n'.join(xml)
    res = '\n'.join(df.apply(row_to_xml, axis=1)).encode('utf-8')

    if filename is None:
        return res
    with open(filename, mode) as f:
        f.write(str(res))

pd.DataFrame.to_xml = to_xml

def write_file(df,f_type,question_num):
    if f_type == 'csv':
        file_name = 'question'+str(question_num)+'.csv'
        df.to_csv(file_name, sep=',', index=False, header=True)
    elif f_type == 'json':
        file_name = 'question'+str(question_num)+'.json'
        df.to_json(file_name, orient='records')
    elif f_type == 'tbl':
        file_name = 'question'+str(question_num)
        df.to_sql(file_name, con=engine, if_exists='replace')
    elif f_type == 'xml':
        file_name = 'question'+str(question_num)+'.xml'
        df.to_xml(file_name)
    else:
        pass


def processing_function(message_received):
    return(" %r" % message_received)

def callback(ch, method, properties,  body):
    print(" [x] Received %r" % body)
    bod = str(processing_function(body))
    f_type = bod.split(',')[1]
    f_type=f_type[:len(f_type)-1]
    loc = bod.split(',')[0]
    loc = loc[2:]+'\''
    loc = loc[1:len(loc)-1]
    l = []
    l = loc.split('\\')
    loc = '/'.join(l)
    con = sqlite3.connect(loc)
    # *****************************************************************************************************************************
    # question1
    # track,band,type
    sql="select  tracks.name,composer , media_types.name as album_type from tracks inner join media_types on tracks.mediatypeid = media_types.mediatypeid;"
    df = pd.read_sql_query(sql, con)
    write_file(df,f_type,1)
    # *****************************************************************************************************************************
    # question 2
    # list of customers:name phone,email,concatanted adress
    sql = "SELECT FirstName || ' '|| LastName,Phone,Email,ifnull(Address,'')||' '||ifnull(PostalCode,'')|| ' ' ||ifnull(State,'')|| ' '|| ifnull(Country,'') as shipment_address FROM customers "
    df = pd.read_sql_query(sql, con)
    write_file(df, f_type, 2)
    # *****************************************************************************************************************************
    # question 3
    # how many different domains in each country
    sql="SELECT country, count(distinct substr(email,instr(Email,'@')+1))  as count_domains FROM customers  group by Country"
    df = pd.read_sql_query(sql, con)
    write_file(df, f_type, 3)
    # *****************************************************************************************************************************
    # question 4
    # how many disks bought each customer
    file_name = 'question4.csv'
    sql = "SELECT FirstName ||' ' || LastName as customer,count(distinct tracks.albumid) as cnt_albums from customers inner join invoices on customers.CustomerId = invoices.CustomerId inner join invoice_items on invoices .InvoiceId = invoice_items.InvoiceId inner JOIN tracks on invoice_items.TrackId = tracks.TrackId group by FirstName ||' ' || LastName"
    df = pd.read_sql_query(sql, con)
    write_file(df, f_type, 4)
    # *****************************************************************************************************************************
    # question 5
    # how many albums sold in eack country
    file_name = 'question5.csv'
    sql = "select country,count(albumid) as cnt_albums_sold FROM( SELECT distinct customers.CustomerId,albumid,country from customers inner join invoices on customers.CustomerId = invoices.CustomerId inner join invoice_items on invoices .InvoiceId = invoice_items.InvoiceId inner JOIN tracks on invoice_items.TrackId = tracks.TrackId )a group by Country "
    df = pd.read_sql_query(sql, con)
    write_file(df, f_type, 5)
    # *****************************************************************************************************************************
    # question 6
    # what is the most popular album in each country
    file_name = 'question6.csv'
    sql = "select country,albums.Title as album,count(distinct customers.customerid) cnt from customers inner join invoices on customers.CustomerId = invoices.CustomerId inner join invoice_items on invoices .InvoiceId = invoice_items.InvoiceId inner JOIN tracks on invoice_items.TrackId = tracks.TrackId inner join albums on tracks.AlbumId = albums.AlbumId group by customers.country, albums.Title"
    df = pd.read_sql_query(sql, con)
    df = df.sort_values(['cnt', 'album'], ascending=False).groupby('Country').head(1)
    df = df.iloc[:, 0:2]
    write_file(df, f_type, 6)
    # *****************************************************************************************************************************
    # question 7
    # what is the most popular album in usa since 2011
    file_name = 'question7.csv'
    sql = "select albums.Title ,count(distinct customers.customerid) cnt from customers inner join invoices on customers.CustomerId = invoices.CustomerId inner join invoice_items on invoices .InvoiceId = invoice_items.InvoiceId inner JOIN tracks on invoice_items.TrackId = tracks.TrackId inner join albums on tracks.AlbumId = albums.AlbumId WHERE country = 'USA' and  strftime('%Y,InvoiceDate') >=2011"
    sql = sql+  " group by   albums.Title order by 2 desc limit 1"
    df = pd.read_sql_query(sql, con)
    df = df.iloc[:, 0:1]
    write_file(df, f_type, 7)
    # *****************************************************************************************************************************
    # question 8
    # customers that their invoices have at least two missing fields
    file_name = 'question8.csv'
    sql = "SELECT DISTINCT customers.FirstName||' '||customers.LastName as customer_bad_invoice FROM  customers "
    sql =sql+"inner JOIN ( select InvoiceId, CustomerId, sum( case ifnull(InvoiceDate,'fieldnull') when 'fieldnull' then 1 else 0 END + "
    sql = sql+"case ifnull(BillingAddress,'fieldnull') when 'fieldnull' then 1 else 0 END + "
    sql = sql+ "case ifnull(BillingCity,'fieldnull') when 'fieldnull' then 1 else 0 END + "
    sql = sql+"case ifnull(BillingState,'fieldnull') when 'fieldnull' then 1 else 0 END + "
    sql = sql+"case ifnull(BillingCountry,'fieldnull') when 'fieldnull' then 1 else 0 END+ "
    sql = sql+"case ifnull(BillingPostalCode,'fieldnull') when 'fieldnull' then 1 else 0 END)as missing "
    sql = sql+"from invoices group by InvoiceId, CustomerId )m on m.CustomerId = customers.CustomerId where m.missing>=2"
    df = pd.read_sql_query(sql, con)
    write_file(df, f_type, 8)
    # *****************************************************************************************************************************

channel.basic_consume(callback,
                      queue='hello',
                      no_ack=True)


print(' [*] Waiting for messages. To exit press CTRL+C')
channel.start_consuming()
