require 'fileutils'
require 'fluent-logger'

def start_fluentd(verbose=false)
  conf = <<-CONF
<source>
  @type forward
</source>

<match test>
  @type file
  path /tmp/fluent-shutdown-test/output/${tag}
  append true

  <buffer tag>
    @type memory
  </buffer>
</match>
CONF

  FileUtils.remove_dir '/tmp/fluent-shutdown-test' if File.exist?('/tmp/fluent-shutdown-test')
  FileUtils.mkdir_p '/tmp/fluent-shutdown-test'
  File.open('/tmp/fluent-shutdown-test/fluent.conf', 'wb') do |file|
    file.puts conf
  end

  pid = fork do
    if verbose
      system("fluentd -c /tmp/fluent-shutdown-test/fluent.conf -d /tmp/fluent-shutdown-test/pid")
    else
      system("fluentd -c /tmp/fluent-shutdown-test/fluent.conf -d /tmp/fluent-shutdown-test/pid -o /dev/null")
    end

    exit
  end
  sleep 3

  pid
end

def shutdown_fluentd(fork_pid)
  pid = File.open('/tmp/fluent-shutdown-test/pid', 'r').read.chomp
  system "kill #{pid}"
  sleep 5
end

def post_events(cnt)
  log = Fluent::Logger::FluentLogger.new(nil, :host => 'localhost', :port => 24224)
  cnt.times do |i|
    log.post("test", {"test" => i})
  end

  log.close
end

def veryfy_output(cnt)
  log_count = File.open("/tmp/fluent-shutdown-test/output/test.log").readlines.count
  log_count == cnt
end

require 'test/unit'
require 'test/unit/ui/console/testrunner'

class TC_FluentdShutdownTest < Test::Unit::TestCase
  def test_10_events
    fork_pid = start_fluentd

    # post 10 events
    post_events 10

    # send graceful shutdown immediately
    shutdown_fluentd(fork_pid)

    # check out_file size
    result = veryfy_output 10
    assert(result, 'post event count != output event size')
  end

  def test_100000_events
    fork_pid = start_fluentd

    # post 100000 events
    post_events 100000

    # send graceful shutdown immediately
    shutdown_fluentd(fork_pid)

    # check out_file size
    result = veryfy_output 100000
    assert(result, 'post event count != output event size')
  end
end

Test::Unit::UI::Console::TestRunner.run(TC_FluentdShutdownTest)
