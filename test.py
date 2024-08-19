from alive_progress import alive_bar
import time
import random

def main():
    num = random.randint(900, 10000)
    with alive_bar(num) as bar:
        for i in range(num):
            time.sleep(random.randint(1, 5))
            bar()

if __name__=='__main__':
    main()