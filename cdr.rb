require 'active_record'
require 'pg'
require 'pry'

#ActiveRecord::Base.logger = Logger.new(STDOUT)
ActiveRecord::Base.establish_connection(
    adapter: 'postgresql',
    host: 'pgiotest1.magnesis.co.uk',
    port: '5432',
    username: 'postgres',
    password: '',
    database: 'telecom'
)

class CdrsToReprocess < ActiveRecord::Base
    self.table_name = 'cdrs_to_reprocess'
    def to_s
        "('#{fingerprint}','#{startofcall.strftime('%F %T')}',#{duration},'#{originatingcli}','#{diallednumber}',#{location_id})"
    end
end

class Cdr_result < ActiveRecord::Base
    self.table_name = 'cdr_result'
end

class Location < ActiveRecord::Base
    has_one :telephoneindex
    self.table_name = 'location'
end

class Telephoneindex < ActiveRecord::Base
    self.table_name = 'telephoneindex'
    has_one :location
    def to_s
        "Telephone index #{self.telephoneindex}"
    end
end

class JobControl < ActiveRecord::Base
    self.table_name = 'job_control'
end

worker_number = ARGV.first.to_i
start_time = Time.now
last_index = '' # Setup last index variable for tracking
while(JobControl.where("worker_id = ? AND completed IS NULL", worker_number).size > 0) do
    loop_start = Time.now
    rows_to_insert = []
    job = JobControl.where("worker_id = ? AND completed IS NULL", worker_number).order(:worker_id, :starting_number).first

    where_sql = "reprocess IS TRUE AND diallednumber >= ? AND diallednumber <= ?" 
    cdrs = CdrsToReprocess.where(where_sql, job.starting_number, job.finishing_number).order(:diallednumber)
    prev_last_index = last_index[0,6] #Truncate to length 6
    last_index = cdrs.last.diallednumber
    indexes = Telephoneindex.where("telephoneindex >= ? AND telephoneindex <= ?", prev_last_index,last_index)
    # Record the previous max telephone index to avoid loading excess records
    
    index_pointer = indexes.length - 1
    cdr_pointer = cdrs.length - 1
    while(cdr_pointer >= 0) do
        cdr = cdrs[cdr_pointer]
        index = indexes[index_pointer]
        if(cdr.diallednumber.include?(index.telephoneindex)) then
            cdr.location_id = index.location.id
            rows_to_insert << cdr
            cdr_pointer -= 1
        else
            index_pointer -= 1
        end
    end
    insert_sql = "INSERT INTO reprocessed_cdrs(fingerprint, startofcall, duration, originatingcli, diallednumber, location_id) "
    insert_sql += "VALUES #{rows_to_insert.join(", ")}"
    ActiveRecord::Base.connection.execute insert_sql
    job.completed = Time.now
    job.save
    p "Loop time: " + (Time.now - loop_start).to_s
end
p "Match Process time: " + (Time.now - start_time).to_s
