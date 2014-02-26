require 'rubygems'
require 'aws-sdk'
require 'tweetstream'

@consumer_key       = ENV['CONSUMER_KEY']
@consumer_secret    = ENV['CONSUMER_SECRET']
@oauth_token        = ENV['OAUTH_TOKEN']
@oauth_token_secret = ENV['OAUTH_TOKEN_SECRET']
@tweet_keywords     = ENV['TWEET_KEYWORDS'].split(',')
@stream_name        = 'tweets'
@kinesis_client     = AWS::Kinesis.new.client

def check_if_already_running
  if DATA.flock(File::LOCK_NB | File::LOCK_EX)
    puts 'Added lock to make sure only one instance of this script is running'
  else
   fail 'An instance of this script is already running'
  end
end

def stream_exists?(stream_name)
  stream = @kinesis_client.describe_stream(:stream_name => stream_name)
  stream[:stream_description][:stream_status] == 'ACTIVE' ? true : false
end

def configure_tweet_stream
  TweetStream.configure do |config|
    config.consumer_key       = @consumer_key
    config.consumer_secret    = @consumer_secret
    config.oauth_token        = @oauth_token
    config.oauth_token_secret = @oauth_token_secret
    config.auth_method        = :oauth
  end
end

def start_tweet_tracking
  puts "starting to consume twitter stream "
  TweetStream::Client.new.track(@tweet_keywords) do |status|
    puts "Tweet Text:     #{status.text}"
    puts "Tweet Language: #{status.lang}"
    puts "User Name:      #{status.user.screen_name}"
    put_tweet_into_kinesis(status.text, status.user.screen_name)
  end
end

def put_tweet_into_kinesis(tweet_text, username)
  res = @kinesis_client.put_record(
    :stream_name => @stream_name, 
    :data => tweet_text, 
    :partition_key => username
  )    
  puts "Put record to Kinesis. Shard: #{res[:shard_id]} - sequence number: #{res[:sequence_number]}"
end

check_if_already_running
fail "stream '#{@stream_name}' must exist for this demo to work" unless stream_exists?(@stream_name)
configure_tweet_stream
start_tweet_tracking

__END__
DO NOT REMOTE: required for the DATA object above.
