nrequire 'rubygems'
require 'aws-sdk'

STREAM_NAME = 'tweets'
STATUS_FILE_PREFIX = 'last_seq_number'

@k = AWS::Kinesis.new.client

def status_file_name(shard_id)
  STATUS_FILE_PREFIX + shard_id
end

def get_iterator(shard_id)
  file_name = status_file_name(shard_id)
  if File.exists?(file_name)
    puts "getting iterator from file"
    f = File.open(file_name,'r')
    start_seq_number = f.read
    f.close
    res = @k.get_shard_iterator(
      :stream_name => STREAM_NAME, 
      :shard_id => shard_id,
      :shard_iterator_type => 'AFTER_SEQUENCE_NUMBER',
      :starting_sequence_number => start_seq_number
    )
    return res[:shard_iterator]
  else
    puts "getting trim horizon"
    res = @k.get_shard_iterator(
      :stream_name => STREAM_NAME,
      :shard_id => shard_id,
      :shard_iterator_type => 'TRIM_HORIZON'
    )
    return res[:shard_iterator]
  end
end


stream = @k.describe_stream(:stream_name => STREAM_NAME)
begin
  stream[:stream_description][:shards].each do |shard|
    shard_id = shard[:shard_id]
    shard_iterator = get_iterator(shard_id)
    last_sequence_number = '' 
    puts "calling get_records 10 times and then wait for a little: "
    10.times do 
      result = @k.get_records(
        :shard_iterator => shard_iterator
      )

      shard_iterator = result[:next_shard_iterator]
      #puts "#{result[:records].count} records returned"
      result[:records].each do |record|
        data = record[:data]
        sequence_number = record[:sequence_number]
        partition_key = record[:partition_key]
    
        puts "Partition key: #{partition_key}"
        puts "Data: #{data}"
        puts "Sequence number: #{sequence_number}"
        last_sequence_number = sequence_number
      end
    end
    if last_sequence_number.length > 0
      puts "Writing last_sequence number '#{last_sequence_number}' to file"
      f = File.open(status_file_name(shard_id),'w')
      f.write(last_sequence_number)
      f.close
    end
    puts "Sleeping 5 seconds"
    sleep 1
  end
end while true  
