require 'rubygems'
require 'aws-sdk'
require 'tweetstream'


if DATA.flock(File::LOCK_NB | File::LOCK_EX)
  puts 'Added lock to make sure only one instance of this script is running'
else
  fail 'An instance of this script is already running'
end

@consumer_key       = ENV['CONSUMER_KEY']
@consumer_secret    = ENV['CONSUMER_SECRET']
@oauth_token        = ENV['OAUTH_TOKEN']
@oauth_token_secret = ENV['OAUTH_TOKEN_SECRET']
@tweet_keywords     = ENV['TWEET_KEYWORDS'].split(',')
@stream_name        = 'tweets'
@kinesis_client     = AWS::Kinesis.new.client

TweetStream.configure do |config|
  config.consumer_key       = @consumer_key
  config.consumer_secret    = @consumer_secret
  config.oauth_token        = @oauth_token
  config.oauth_token_secret = @oauth_token_secret
  config.auth_method        = :oauth
end

def validate_kinesis_stream(stream_name)
  status = stream_status(stream_name)
  if status == 'ACTIVE' 
    return
  elsif status == 'STREAM_DOES_NOT_EXIST'
    # create stream if it doesn't exist
    stream = @kinesis_client.create_stream(
      :stream_name => stream_name,
      :shard_count => 2
    )
    puts 'Waiting 30 seconds for stream to become active'
    sleep 30
  end 
  #try again to see if status is active now
  status = stream_status(stream_name)
  fail "Stream is not in the 'ACTIVE' state. State: '#{status}'" unless status == 'ACTIVE'
end

def stream_status(stream_name)
  begin
    stream = @kinesis_client.describe_stream(:stream_name => stream_name)
    return stream[:stream_description][:stream_status]
  rescue AWS::Kinesis::Errors::ResourceNotFoundException => ex
    return 'STREAM_DOES_NOT_EXIST'
  end
end

validate_kinesis_stream(@stream_name)
puts "starting to consume twitter stream "
TweetStream::Client.new.track(@tweet_keywords) do |status|
  puts "Tweet Text:     #{status.text}"
  puts "Tweet Language: #{status.lang}"
  puts "User Name:      #{status.user.screen_name}"
  res = @kinesis_client.put_record(:stream_name => 'tweets', :data => status.text, :partition_key => status.user.screen_name)    
  puts "Put record to Kinesis. Shard: #{res[:shard_id]} - sequence number: #{res[:sequence_number]}"
end

__END__
DO NOT REMOTE: required for the DATA object above.
