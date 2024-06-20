class A():
    def t(self, func):
        self.a = 1
        func(self)

def b(n):
    print(n.a)

a = A()
a.t(b)