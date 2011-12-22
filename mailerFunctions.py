#!/usr/bin/env python
# -*- coding: ascii -*-
'''Functions for emailing.'''

from email.MIMEMultipart import MIMEMultipart
from email.MIMEBase import MIMEBase
from email.MIMEText import MIMEText
from email.Utils import COMMASPACE, formatdate
from email import Encoders
import os
import smtplib
import datetime
import logging
		
class mailer:
	def __init__(self,SERVER="your.server.com",FROM="your@email.com"):
		self.server = SERVER
		self.send_from = FROM
		self.logger = logging.getLogger('mailer')

	def send_mail(self, send_to, subject, text, files=[]):
		assert type(send_to)==list
		assert type(files)==list
		if self.logger.isEnabledFor(logging.DEBUG):
			self.logger.debug(' '.join(("Sending email to:",' '.join(send_to))))
			self.logger.debug(' '.join(("Subject:",subject)))
			self.logger.debug(' '.join(("Text:",text)))
			self.logger.debug(' '.join(("Files:",' '.join(files))))
		msg = MIMEMultipart()
		msg['From'] = self.send_from
		msg['To'] = COMMASPACE.join(send_to)
		msg['Date'] = formatdate(localtime=True)
		msg['Subject'] = subject
		msg.attach( MIMEText(text) )
		for f in files:
			part = MIMEBase('application', "octet-stream")
			part.set_payload( open(f,"rb").read() )
			Encoders.encode_base64(part)
			part.add_header('Content-Disposition', 'attachment; filename="%s"' % os.path.basename(f))
			msg.attach(part)
		smtp = smtplib.SMTP(self.server)
		mydict = smtp.sendmail(self.send_from, send_to, msg.as_string())
		if self.logger.isEnabledFor(logging.DEBUG):
			self.logger.debug("Email Successfully Sent!")
		smtp.close()
		return mydict
