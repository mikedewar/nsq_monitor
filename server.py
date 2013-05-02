import tornado.web
import os
import tornado.options
import logging
import os

import utils

class TopicHandler(tornado.web.RequestHandler):
    def get(self, topic): 
        logging.info(topic)
        print "topic", topic
        print "not topict", not topic
        topics = utils.get_topics()
        if topic:
            topics.pop(topics.index(topic))
        data = utils.get_data_dict(topic)
        params = {
            "topic": topic,
            "topics": topics,
            "data": data,
        }
        self.render('topic.html',**params)

        

settings = {
    "debug": True,
    "template_path": os.path.join(os.path.dirname(__file__),"templates/"),
    "static_path": os.path.join(os.path.dirname(__file__),"static/"),
}

application = tornado.web.Application(
    [
        (r"/topic/(.*)", TopicHandler),
    ],
    **settings
)

if __name__ == "__main__":
    tornado.options.parse_command_line()
    logging.info("starting tornado web server")
    application.listen(8888)
    tornado.ioloop.IOLoop.instance().start()
