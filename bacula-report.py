# coding: utf-8
# /usr/bin/python
import sys
import subprocess
from subprocess import Popen, PIPE
import MySQLdb
import smtplib
from datetime import date
from email import encoders
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.utils import COMMASPACE, formatdate

cur_time = date.today()  # retorna vetor contendo a data
# formata data e retorna valor correspondente ao dia
today = cur_time.strftime('%Y-%m-%d')

query_log = """select Job.Type,Pool.Name,Job.StartTime,Job.EndTime,Job.JobFiles,Job.JobErrors,Job.JobMissingFiles,Job.PurgedFiles,Status.JobStatusLong,Status.Severity,Client.Uname,Log.LogText \
    from Job \
    inner join Status on Job.JobStatus = Status.JobStatus \
    inner join Pool on Job.PoolId = Pool.PoolId \
    inner join File on Job.JobId = File.JobId \
    inner join Client on Job.ClientId = Client.ClientId \
    inner join Log on Job.JobId = Log.JobId \
    where Job.JobStatus <> 'T' and Job.SchedTime like '{}%' \
    order by Job.SchedTime desc""".format(today)  # query SQL do log

query_log_errors = """select Name, count(JobStatus) as Errors \
    from Job \
    group by Name \
    having JobStatus in ('A','B','E','I','e','f') and SchedTime = '{}%'""".format(today)  # query SQL dos erros

flags = ['Type', 'Pool', 'Start time', 'End time', 'Files', 'Erros', 'Missing files',
         'Purged files', 'Job status', 'Severity', 'System', 'Log']  # flags para exibição do log
flags_errors = ['Server', 'Errors']  # flags de erro para log

from_addr = 'reportbackup@brdefender.com.br'  # remetente
to_addrs = ['suporte@brdefender.com.br',
            'noc@brdefender.com.br']  # destinatários


def getFileset():  # retorna arquivos gerados durante processos de backup
    shellcmd = "ls -lh --time-style=+%D /mnt/repositorio | grep $(date +%D) | awk '{ print $7, $5 }'"
    # executa comando com filtros para restringir resultado a arquivos criados no dia atual
    response = Popen([shellcmd], stdout=PIPE,
                     stderr=subprocess.STDOUT, shell=True)
    # resultado do comando acima atribuído a duas variáveis dependendo do retorno se bem sucedido
    stdout, stderr = response.communicate()

    with open('files.txt', 'w') as file:  # cria arquivo e insere dados obtidos acima
        if not stdout:
            # escreve no arquivo que nenhum arquivo foi gerado
            file.write('Não foram gerados arquivos!')
            return None

        if not response.poll():  # verifica se processo filho foi executado devidamente
            # escreve string no arquivo
            file.write('Arquivos gerados durante processos: \n')

            # escreve nome do arquivo de fileset tape do bacula
            file.write('{}\n'.format(stdout))
        else:
            # escreve erro no arquivo caso condição não seja correspondida
            file.write(str(stderr))


def get_records():  # retorna dados do banco
    try:
        connection = MySQLdb.connect(
            'localhost', 'report', 'bacula@@mysql', 'bacula')  # conecta com banco
        cursor = connection.cursor()  # cria cursor

        cursor.execute(query_log)  # executa query
    except Exception:
        print >> open('report.log', 'w'), sys.exc_info()
        # escreve erro para ser enviado no email
        sys.exit(1)
    else:
        data = cursor.fetchall()  # retorna resultado da query

        if data:
            with open('report.log', 'a+') as file:  # cria arquivo de log
                file.write(
                    'Relatório dos backups realizados no dia: {}\n\n'.format(today))

                for row in data:
                    aux = 0

                    for column in row:  # iteração realizada devido a matriz retornada através da query
                        # escreve no arquivo criado acima coluna a coluna
                        file.write('{}: {}\n'.format(flags[aux], str(column)))

                        aux += 1

                    file.write('\n')

                file.flush()  # limpa buffer I/O

            # retorna resultado da query de erro
            cursor.execute(query_log_errors)

            result = cursor.fetchall()

            if result:
                with open('report.log', 'a+') as file:  # edita arquivo de log
                    aux = 0

                    for row in result:
                        for column in row:
                            file.write('{}: {}'.format(
                                flags_errors[aux], str(column)))
                            # escreve no arquivo de log estatística de erros
                            aux += 1
        else:
            print >> open('report.log', 'w'), 'Não houve erros no backup do dia {}'.format(
                date.today())  # escreve outra coisa no arquivo dependendo das condições dos laços
            cursor.close()  # finaliza conexão com banco


def format_msg():  # formata mensagem de e-mail
    msg = MIMEMultipart()  # instancia e define o tipo da mensagem MIME
    msg['From'] = 'reportbackup@brdefender.com.br'  # remetente
    msg['To'] = COMMASPACE.join(to_addrs)  # destinatário
    msg['Date'] = formatdate(localtime=True)  # data da mensagem
    # assunto da mensagem
    msg['Subject'] = 'Verificação dos backups: Master Cabos Bacula'

    # conteúdo da mensagem
    msg.attach(MIMEText(open('report.log', 'r').read()))
    msg.attach(MIMEText(open('files.txt', 'r').read()))

    # part = MIMEApplication(open('report.log','rb').read()) # lê conteúdo do arquivo

    # encoders.encode_base64(part) # muda o enconding do arquivo
    # part.add_header('Content-Disposition','attachment; filename="report.log"') # anexa arquivo
    # msg.attach(part) # inclui arquivo na mensagem

    return msg  # retorna valor da mensagem


def send_mail(msg):  # envia relatório por servidor de e-mail SMTP
    try:
        # cria conexão com servidor de envio de e-mail
        smtp_server = smtplib.SMTP(
            host='###.###.###.###', port=587, timeout=None)

        smtp_server.ehlo()  # identifica host
        smtp_server.starttls()  # inicia conexão tls
        smtp_server.ehlo()  # identifica credenciais novamente
        smtp_server.login(from_addr, 'lPthMrUjZn')  # efetua login no servidor
    except smtplib.SMTPException as smtpErr:
        print >> open('report.log', 'w'), sys.exc_info()[0], smtpErr.message
        # escreve erro para ser enviado no email
        sys.exit(1)
    else:
        # envia mensagem de e-mail
        smtp_server.sendmail(from_addr, to_addrs, msg.as_string())
        smtp_server.close()  # finaliza conexão com servidor


if __name__ == "__main__":
    get_records()  # retorna dados do banco
    getFileset()  # retorna arquivos criados no dia atual

    msg = format_msg()  # recebe mensagem formatada

    send_mail(msg)  # envia e-mail

    # remove arquivo de log e arquivos tape gerados
    subprocess.call(['rm', 'files.txt', 'report.log'])
