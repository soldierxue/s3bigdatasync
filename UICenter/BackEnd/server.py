import BaseHTTPServer
import ddbModel
import sys
import json

class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header("Content-type","application/json")
        self.end_headers()
        try:
            #redirect stdout to client
            stdout=sys.stdout
            sys.stdout=self.wfile
            self.makepage()
        finally:
            sys.stdout=stdout #restore
    
    def makepage(self):
        inputTime = self.path[1:]
        
        print json.dumps(ddbModel.getItemsAfterTime(inputTime))

def main():
    PORT = 8000
    httpd = BaseHTTPServer.HTTPServer(("", PORT), Handler)
    print "serving at port", PORT
    httpd.serve_forever()

if __name__ == '__main__':
    main()
