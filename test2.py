import threading

def thread(ref):
    print(ref)
    ref = 2

class A():
    def __init__(self) -> None:
        self.a = None 

        t = threading.Thread(target=thread, args=(self.a,))
        t.start()


a = A()
while True:
    print(a.a)