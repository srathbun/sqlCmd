## sqlCmd.py
## Author:   Spencer Rathbun
## Date:     18 January 2011
## Version:  1.3
## Original code copied from James Thiele's console.py at http://code.activestate.com/recipes/280500-console-built-with-cmd-object/
## As much as possible, commands are atomic
## That is, they do not depend on each other
## The only exception is the data structure manipulation,
## which can depend on previous data being entered into the structure
## This is a safety valve in case data fails to exist, therefore a work order will not be created


import os, sys, cmd, ConfigParser, datetime, re, string, shlex, time, logging
import pyodbc
from decimal import *
import argparse
import mailerFunctions

class sqlCmd(cmd.Cmd):
	"""Program to parse sql file and execute commands therein using pyodbc."""
	
	def __init__(self, args):
		cmd.Cmd.__init__(self)
		# disable autocompletion if you don't have the readline module imported
		self.completekey = None
		self.stdin = args.infile
		self.stdout = sys.stdout
		self.debug = args.d
		self.logger = logging.getLogger('sqlCmd')
		# Disable rawinput module use
		self.use_rawinput = False
		self.datafile = "" # add datafile variable
		# Do not show a prompt after each command read
		self.stack = []
		self.rows = ''
		self.current = ''
		self.register1 = ''
		self.register2 = ''
		self.register3 = ''
		self.register4 = ''
	
		## Create database connection for commands ##
		## autocommit is set to false by default ##
		## so we can treat it as a standard commit/rollback sequence ##
		
		SERVER = args.s
		connString = 'DRIVER={'+args.driver+'};SERVER='+SERVER+';DATABASE='+args.db+';UID='+args.user+';pooling=false;password='+args.password+';'+args.e
		
		if self.debug:
			self.logger.debug(connString +'\n')
			self.logger.debug(args.info +'\n')
		
		try:
			self.db = pyodbc.connect(connString)
			self.cur = self.db.cursor()
		except pyodbc.Error, e:
			self.logger.debug(str(e))

		self.prompt = ''
		if args.p:
			self.prompt = '(@'+SERVER+')>'
		
		self.config = ConfigParser.SafeConfigParser()
		try:
			self.config.readfp(open(args.info))
		except:
			self.config.add_section('workOrder')
		
	def rollbackAndCloseDb(self):
		"""rollback changes made to the database and close the connection"""
		self.db.rollback()
		self.cur.close()
		self.db.close()
	
	def commitAndCloseDb(self):
		"""commit changes made to the database and close the connection"""
		try:
			self.db.commit()
			self.cur.close()
			self.db.close()
		except AttributeError, e:
			self.logger.debug(str(e))
	
	def returnValue(self):
		"""used to pass the work order id number back to the calling program
		may be deprecated in the future for built in emailing"""
		if self.config.has_option('workOrder', 'OrderID'):
			return int(self.config.get('workOrder', 'OrderID'))
		else:
			return 0
	
		## default Command definitions ##
	def do_hist(self, args):
		"""Print a list of commands that have been entered"""
		print self._hist
		
	def do_exit(self, args):
		"""Exits from the console"""
		return -1

	## Command definitions to support Cmd object functionality ##
	def do_EOF(self, args):
		"""Exit on system end of file character"""
		return self.do_exit(args)
		
	def do_shell(self, args):
		"""Pass command to a system shell when line begins with !"""
		os.system(args)

	def do_help(self, args):
		"""Get help on commands
		'help' or '?' with no arguments prints a list of commands for which help is available
		'help <command>' or '? <command>' gives help on <command>
		"""
		## The only reason to define this method is for the help text in the doc string
		cmd.Cmd.do_help(self, args)

	## Override methods in Cmd object ##
	def preloop(self):
		"""Initialization before prompting user for commands.
		Despite the claims in the Cmd documentation, Cmd.preloop() is not a stub.
		"""
		cmd.Cmd.preloop(self)   ## sets up command completion
		self._hist    = []      ## No history yet
		self._locals  = {}      ## Initialize execution namespace for user
		self._globals = {}
		
	def postloop(self):
		"""Take care of any unfinished business.
		Despite the claims in the Cmd documentation, Cmd.postloop() is not a stub.
		"""
		cmd.Cmd.postloop(self)   ## Clean up command completion
		self.commitAndCloseDb()	## if there have been no errors, commit the transaction

	def precmd(self, line):
		""" This method is called after the line has been input but before
			it has been interpreted. If you want to modify the input line
			before execution (for example, variable substitution) do it here.
		"""
		self._hist += [ line.strip() ]
		mylist = re.split(r'(\'|\s)', line)
		output = ""
		if self.debug:
			self.logger.debug(line +'\n')
		try:
			if line.startswith('REM'):
				return "REM"
			elif line.startswith('IF'):
				pass
			elif line.startswith('repeat'):
				return line
			elif line.startswith('parseList'):
				mylist[2] = self.config.get("workOrder", mylist[2][2:])
				if mylist[2] == "":
					output = "REM"
				else:
					output = ''.join(mylist)
				return output

			
			for item in mylist:
				if (string.find(item, '@$') != -1):
					output = output + self.config.get("workOrder", item[2:])
				elif (string.find(item, 'STACK') != -1):
					output = output + str(self.stack.pop())
				elif (string.find(item, 'REG1') != -1):
					output = output + self.register1
				elif (string.find(item, 'REG2') != -1):
					output = output + self.register2
				elif (string.find(item, 'REG3') != -1):
					output = output + self.register3
				elif (string.find(item, 'REG4') != -1):
					output = output + self.register4
				elif (string.find(item, 'ALG') != -1):
					alg = shlex.split(item)
					output = output + eval(''.join(alg[1:]))
				else:
					output = output + item
			return output
		except Exception,e:
			if (self.stdin != sys.stdin):
				raise e
			else:
				print e
				return ""

	def postcmd(self, stop, line):
		"""If you want to stop the console, return something that evaluates to true.
		If you want to do some post command processing, do it here.
		"""
		return stop

	def onecmd(self, str):
		"""test of onecmd for catching of runtime exceptions"""
		try:
			rtn = cmd.Cmd.onecmd(self, str)
			return rtn
		except Exception, e:
			if (self.stdin != sys.stdin):
				raise e
			else:
				print e
				return 0
	
	def emptyline(self):    
		"""Do nothing on empty input line"""
		pass

	def default(self, line):       
		"""Called on an input line when there is no wrapper command.
		We then pass it directly onto the database with nothing other than
		variable substitution, and print any results
		"""
		try:
			self.cur.execute(line)
			if (self.cur.description != None):
				self.rows = self.cur.fetchall()
			else:
				self.rows = []
		except Exception,e:
			if (self.stdin != sys.stdin):
				raise e
			else:
				print e

########## SQL mini language definitions ########################################

	def do_python(self, args):
		"""run python command in the current scope. WARNING! This has complete python access and all privileges of the currently running user!"""
		try:
			exec(args) #in self._locals, self._globals
		except Exception, e:
			print e.__class__, ":", e
			
	def do_set(self, args):
		"""Set a config item. args[1] determines how."""
		try:
			list = shlex.split(args)
			if list[1] == "select":
				self.cur.execute(' '.join(list[1:]))
				self.config.set(list[0], self.cur.description[0][0], str(self.cur.fetchone()[0]))
			elif list[1] == "=":
				self.config.set(list[0], list[2], self.config.get(list[3], list[4]))			
			elif list[1] == "pop":
				self.config.set(list[0], list[2], str(self.stack.pop()))
			else:
				self.config.set(list[0], list[1], list[2])
		except Exception, e:
			print "Failure setting variable: ", e
	
	def do_build(self, args):
		"""Create a new section in config"""
		self.config.add_section(args)
		
	def do_get(self, args):
		"""From section args[0] get args[1]"""
		try:
			list = shlex.split(args)
			print self.config.get(list[0], list[1])
		except Exception, e:
			print e
		
	def do_getAll(self, args):
		"""print all of the items in the section args[1]"""
		try:
			for item in self.config.items(args):
				print item
		except Exception, e:
			print e
	
	def do_parseList(self, args):
		"""Import the contents of args[1] to working space, then run the command created by the rest of args. Afterwards, check if we need to recurse to another node. WARNING! can infinite loop if node never gets an empty string value!"""
		list = re.split(r'(\s)', args)
		for item in self.config.items(list[0]):
			self.config.set("workOrder", item[0], item[1])
		## only run the command once
		## and only after the sub items are added to the workOrder section
		self.onecmd(self.precmd(''.join(list[2:])))
		## now check if there is another node in the list
		if self.config.has_option(list[0], "next"):
			self.do_parseList(self.config.get(list[0], "next")+" "+''.join(list[2:]))
		
	def do_repeat(self, args):
		"""repeat the command specified in the second command onward, on the contents of the items in argument one"""
		while self.stack:
			self.onecmd(self.precmd(args))
	
	def do_pushAllRows(self, args):
		"""push all the items in each row of the current selection onto the stack, order is left to right and top to bottom"""
		try:
			for row in self.rows:
				for item in row:
					self.stack.append(item)
		except Exception,e:
			print "Failure pushing onto stack: ", e
			
	def do_pushAll(self, args):
		"""push the contents of one section in config onto the stack"""
		try:
			list = shlex.split(args)
			for item in self.config.items(list[0]):
				self.stack.append(item[1])
		except Exception, e:
			print e
			print "Failure during push to stack. Partial data may be on the stack. Attempting to flush stack..."
			self.stack = []
			print "Stack flushed, all contents deleted."
	
	def do_flush(self, args):
		"""delete the contents of the stack"""
		#print "Attempting to flush stack..."
		self.stack = []
		#print "Stack flushed, all contents deleted."
	
	def do_push(self, args):
		"""push an item onto the top of the stack, runs select 'args' and adds the first column in the first row"""
		try:
			self.cur.execute("select "+args)
			self.stack.append(self.cur.fetchone()[0])
		except Exception,e:
			print "Failure pushing onto stack: ", e
	
	def do_pop(self, args):
		"""pop an item off the stack and print it"""
		try:
			print self.stack.pop()
		except:
			print "Stack is empty"
	
	def do_reg(self, args):
		"""set contents of a register. Syntax: reg 1-4 item"""
		list = shlex.split(args)
		if list[0] == '1':
			self.register1 = str(list[1])
		elif list[0] == '2':
			self.register2 = str(list[1])
		elif list[0] == '3':
			self.register3 = str(list[1])
		elif list[0] == '4':
			self.register4 = str(list[1])
		else:
			print "failure changing register:", list[0], list[1]
	
	def do_listReg(self, args):
		"""print the contents of the four registers"""
		print self.register1
		print self.register2
		print self.register3
		print self.register4
	
	def do_sendToFile(self, args):
		"""output the row description and all the rows of the current selection to a file."""
		output = open(args, 'at')
		for item in self.cur.description:
			output.write(item[0] + "|")
		output.write("\n")
		for row in self.rows:
			for item in row:
				try:
					output.write(''.join(s for s in str(item) if s in string.printable))
				except:
					output.write(str(item).encode('utf_16_le'))
				output.write('|')
			output.write("\n")
		output.close()
		
	def do_rollback(self, args):
		"""rollback all previous interactions with the db. NOTE! This does not decrement the autoincrementing ID numbers!"""
		self.db.rollback()
		#return self.do_exit(args)

	def do_printDecode(self, args):
		"""print all the rows in the current selection, inline"""
		for row in self.rows:
			for item in row:
				try:
					print ''.join(s for s in str(item) if s in string.printable),
				except:
					print "PROBLEM!"
				print '|',
			print "\n",
		
	def do_print(self, args):
		"""print all the rows in the current selection, inline"""
		for row in self.rows:
			for item in row:
				try:
					print item, 
				except:
					print item.encode('utf_16_le'),
				print '|',
			print "\n",
	
	def do_printOne(self, args):
		"""print the first row from the current selection"""
		row = self.rows[0]
		for item in row:
			try:
				print item, 
			except:
				print item.encode('utf_16_le'),
			print '|',
		print "\n",
	
	def do_desc(self, args):
		"""print the column names for the current selection set, if any. 'desc 1' prints them in line, 'desc' prints details."""
		for item in self.cur.description:
			if (args == "1"):
				try:
					print ''.join(s for s in str(item[0]) if s in string.printable),
				except:
					print "BADVALUE"
				print '|',
			else:
				try:
					print ''.join(s for s in str(item[0]) if s in string.printable)
				except:
					print "BADVALUE"
		print "\n"
		
	def do_mailAll(self, args):
		"""mail all current rows to your@email.com"""
		MyMailer = mailerFunctions.mailer()
		TO = ["your@email.com"] # must be a list
		SUBJECT = "Current list of pending approvals"
		TEXT = ""
		if len(self.rows) > 0:
			for row in self.rows:
				for item in row:
					try:
						TEXT = TEXT + str(item) + ' ' 
					except:
						print item
						TEXT = TEXT + str(item).encode('utf_16_le') + ' '
					TEXT = TEXT + '| '
				TEXT = TEXT + "\n"
			MyMailer.send_mail(TO,SUBJECT,TEXT)
		del MyMailer
		
	def do_REM(self, args):
		"""This command creates a comment, which will not be sent to the db and will be ignored. """
		pass
	

def main(argv):
	parser = argparse.ArgumentParser(description='Connect to an MSsql database and interpret sqlcmd minilanguage.', version='%(prog)s 1.3')
	parser.add_argument('infile', nargs='?', type=argparse.FileType('r'), default=sys.stdin, help='Take commands from this file, or default to stdin')
	parser.add_argument('-i', '--info', default="", help='Info file for config data imported on startup, if any')
	parser.add_argument('-s', metavar='server', nargs='?', default='', const='', help='Set the server to connect to. Default is the test server, with no input its the live server.')
	parser.add_argument('-p', action='store_true', help='Turn the prompt on')
	parser.add_argument('--driver', help='change the default driver', default='SQL SERVER')
	parser.add_argument('--db', help='change the default database', default='')
	parser.add_argument('--user', nargs='?', help='change the default user', default='', const='')
	parser.add_argument('--password', help='use a password with the username', default='')
	parser.add_argument('-d', action='store_true', help='turn on debugging information')
	parser.add_argument('-e', metavar='extra', type=str, default='', help='extra odbc arguments')
	args = parser.parse_args(argv)
	
	try:
		myInterpreter = sqlCmd(args)
		myInterpreter.cmdloop()
		return myInterpreter.returnValue()
	except Exception,e:
		logging.debug("Failure making Work Order! Printing command history and error message...")
		logging.debug(str(e))
		myInterpreter.do_hist("")
		myInterpreter.rollbackAndCloseDb()
		raise e
	finally:
		args.infile.close()
	
if __name__ == '__main__':
	logging.basicConfig(level=logging.DEBUG,
						format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s',
						datefmt='%m-%d %H:%M')
	console = logging.StreamHandler()
	console.setLevel(logging.INFO)
	formatter = logging.Formatter('%(name)-12s: %(levelname)-8s %(message)s')
	console.setFormatter(formatter)
	logging.getLogger('').addHandler(console)
	sys.exit(main(sys.argv[1:]))
