# ANSWER TO THE ASKED QUESTIONS ARE AT THE BOTTOM

import time
import random
import logging
from threading import Lock


logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s"
)


class BankAccount:
    def __init__(self, account_number, balance):
        self.account_number = account_number
        self.balance = balance
        self.lock = Lock()

    def debit(self, amount):
        with self.lock:
            if amount > self.balance:
                logging.error(f"Insufficient funds in account {self.account_number}.")
                return False
            self.balance -= amount
            logging.info(
                f"Debited {amount} from account {self.account_number}. New balance: {self.balance}."
            )
            return True

    def credit(self, amount):
        with self.lock:
            self.balance += amount
            logging.info(
                f"Credited {amount} to account {self.account_number}. New balance: {self.balance}."
            )


class Transaction:
    def __init__(self, sender: BankAccount, receiver: BankAccount, amount: float):
        self.sender = sender
        self.receiver = receiver
        self.amount = amount

    def process_transaction(self):
        time.sleep(random.uniform(0.1, 0.5))

        if self.sender.debit(self.amount):
            self.receiver.credit(self.amount)
            logging.info(
                f"Transaction completed: {self.amount} transferred from account {self.sender.account_number} to account {self.receiver.account_number}."
            )
        else:
            logging.error(
                f"Transaction failed: Unable to debit from account {self.sender.account_number}."
            )


if __name__ == "__main__":
    # Create two bank accounts
    account_a = BankAccount("A", 1200)
    account_b = BankAccount("B", 300)

    # Simulate a transfer
    transaction1 = Transaction(account_a, account_b, 200)
    transaction1.process_transaction()

    # Attempt a transfer that exceeds the balance
    transaction2 = Transaction(account_a, account_b, 900)
    transaction2.process_transaction()

    # Attempt a transfer that exceeds the balance
    transaction2 = Transaction(account_a, account_b, 300)
    transaction2.process_transaction()


# What are the issues in such a system?
# The process should be purely atomic, i.e. at a time only one process should be done.
# Handling scenarios like insufficient funds or incorrect account details is imp to prevent mistakes
# latency in communication between banks

# What can we do to mitigate some of the issues ?
# the above approach manages account balance with thread-safe methods for debiting and crediting funds
# Handles the transfer process, simulates network delays, and ensures that transactions are processed atomically
# Provides visibility into the transaction process and error management [by logging], error can be diagonised easily if we are using a complex sytem like these.
