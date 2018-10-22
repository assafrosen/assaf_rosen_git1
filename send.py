import pika
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()
channel.queue_declare(queue='hello')
channel.basic_publish(exchange='',
                      routing_key='hello',
                      body='C://sqlite//db//chinook.db,csv')
print(" [x] Sent 'database and file type sent'")
connection.close()