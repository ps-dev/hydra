package hydra.kafka.util

object ValidationUtils {

  val slackChannelRegex: String = "^#[a-z][a-z_-]{0,78}$"

  def isValidSlackChannel(slackChannel: String): Boolean = {
    slackChannel.matches(slackChannelRegex)
  }

}
