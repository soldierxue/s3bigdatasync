import BaseHTTPServer
import ddbModel
import sys
import json

class Handler(BaseHTTPServer.BaseHTTPRequestHandler):
    def do_GET(self):
        if self.path == "/totalProgress":
            self.send_response(200)
            self.send_header("Content-type","application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET")
            self.end_headers()
            try:
                #redirect stdout to client
                stdout = sys.stdout
                sys.stdout = self.wfile
                self.printTotalProgress()
            finally:
                sys.stdout = stdout #restore
        elif self.path == "/tasksGraph":
            self.send_response(200)
            self.send_header("Content-type","application/json; charset=utf-8")
            self.send_header("Access-Control-Allow-origin", "*")
            self.send_header("Access-Control-Allow-Methods", "GET")
            self.end_headers()
            try:
                #redirect stdout to client
                stdout = sys.stdout
                sys.stdout = self.wfile
                self.printTasksGraph()
            finally:
                sys.stdout = stdout #restore
        else:
            self.send_error(404,"file not found");
            return
    
    def printTotalProgress(self):
        print json.dumps(ddbModel.returnTotalProgressData())
        
    def printTasksGraph(self):
        print json.dumps(ddbModel.returnTasksGraphData())

def main():
    PORT = 8000
    httpd = BaseHTTPServer.HTTPServer(("", PORT), Handler)
    print "serving at port", PORT
    httpd.serve_forever()

if __name__ == '__main__':
    main()
