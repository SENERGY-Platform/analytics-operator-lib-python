class A():
    def t(self, func):
        self.a = 1
        func(self)

def b(n):
    print(n.a)
    n.a = 2

a = A()
a.t(b)
print(a.a)