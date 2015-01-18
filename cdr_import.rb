require 'active_record'
require 'pg'
require 'pry'

ActiveRecord::Base.establish_connection(
    adapter: 'postgresql',
    host: 'pgsandbox.magnesis.co.uk',
    port: '5432',
    username: 'postgres',
    password: '',
    database: 'telecom'
)

class CdrsToImport < ActiveRecord::Base
    self.table_name = 'cdr_loadbuffer'
end

class JobControl < ActiveRecord::Base
    self.table_name = 'job_control'
end

worker_number = ARGV.first.to_i
start_time = Time.now
while(JobControl.where("worker_id = ? AND completed IS NULL", worker_number).size > 0) do
    loop_start = Time.now
    job = JobControl.where("worker_id = ? AND completed IS NULL", worker_number).order(:worker_id, :starting_number).first
    insert_sql = "SELECT fn_processcdrbuffer('#{job.starting_number}', '#{job.finishing_number}');"
    ActiveRecord::Base.connection.execute insert_sql
    job.completed = Time.now
    job.save
    p "Loop time: " + (Time.now - loop_start).to_s
end
p "Match Process time: " + (Time.now - start_time).to_s
