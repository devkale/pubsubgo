package main

import (
	"fmt"
	"time"
)

type Subscription struct {
	id           int
	tocall       func(int, int)
	topicid      int
	sentMessages []int
}

type Topic struct {
	id            int
	subscriptions []int
}

func main() {

	createTopic(123)
	addSubscription(123, 1)
	addSubscription(123, 2)
	addSubscription(123, 3)
	subscribe(1, Subscriber1)
	subscribe(2, Subscriber2)
	subscribe(3, Subscriber3)
	publish(123, 567)
	unSubscribe(1)
	publish(123, 789)
	publish(124, 901)
	//deleteSubscription(1)
	//publish(123, "m1")
}

var topics = make(map[int]*Topic)
var subscriptions = make(map[int]*Subscription)

func createTopic(topicID int) {
	var topic Topic
	topic.id = topicID
	topic.subscriptions = []int{}
	topics[topicID] = &topic
}

func deleteTopic(topicID int) {
	delete(topics, topicID)
}

func addSubscription(topicID, subscriptionID int) {
	var subscription Subscription
	subscription.id = subscriptionID
	subscription.topicid = topicID
	topics[topicID].subscriptions = append(topics[topicID].subscriptions, subscriptionID)
	subscriptions[subscriptionID] = &subscription
}

func deleteSubscription(SubscriptionID int) {
	var topicid = subscriptions[SubscriptionID].topicid
	var topic = topics[topicid]
	var indexToRemove = -1
	for i := 0; i < len(topic.subscriptions); i++ {
		if topic.subscriptions[i] == SubscriptionID {
			indexToRemove = i
			break
		}
	}
	topic.subscriptions[indexToRemove] = topic.subscriptions[len(topic.subscriptions)-1]
	topic.subscriptions = topic.subscriptions[:len(topic.subscriptions)-1]
	delete(subscriptions, SubscriptionID)
}

func publish(topicID int, messageID int) {
	var topic = topics[topicID]
	if topic == nil {
		fmt.Println("topic does not exist")
		return
	}
	//fmt.Println(topic)
	for i := 0; i < len(topic.subscriptions); i++ {
		if subscriptions[topic.subscriptions[i]].tocall != nil {
			subscriptions[topic.subscriptions[i]].sentMessages = append(subscriptions[topic.subscriptions[i]].sentMessages, messageID)
			subscriptions[topic.subscriptions[i]].tocall(topic.subscriptions[i], messageID)
		}
	}
}

func subscribe(SubscriptionID int, SubscriberFunc func(int, int)) {
	var subscription = subscriptions[SubscriptionID]
	subscription.tocall = SubscriberFunc
}

func Subscriber1(subscriptionID, messageid int) {
	//fmt.Printf("Subscriber1 called with %d \n", messageid)
	//time.Sleep(30 * time.Second)
	fmt.Printf("Subscriber1 received %d \n", messageid)
	ack(subscriptionID, messageid)

}

func ack(subscriptionID int, messageID int) {
	var subscription = subscriptions[subscriptionID]
	fmt.Printf("Ack The subscripton  id %d message id %d\n", subscriptionID, messageID)
	if len(subscription.sentMessages) == 0 {
		return
	}
	var indexToRemove = -1

	for i := 0; i < len(subscription.sentMessages); i++ {
		if subscription.sentMessages[i] == messageID {
			indexToRemove = i
			break
		}
	}

	subscription.sentMessages[indexToRemove] = subscription.sentMessages[len(subscription.sentMessages)-1]
	subscription.sentMessages = subscription.sentMessages[:len(subscription.sentMessages)-1]

}

func unSubscribe(subscriptionID int) {
	subscriptions[subscriptionID].tocall = nil
}

func Subscriber2(subscriptionID, messageid int) {
	fmt.Printf("Subscriber2 received %d \n", messageid)
	ack(subscriptionID, messageid)
}

func Subscriber3(subscriptionID, messageid int) {
	fmt.Printf("Subscriber3 received %d \n", messageid)
	ack(subscriptionID, messageid)
}

func retry(subscriptionID int, messageID int) {

	time.AfterFunc(time.Duration(20)*time.Second,

		func() {
			fmt.Printf("retry called. The subscripton  id %d message id %d\n", subscriptionID, messageID)
			var indexToRemove = -1

			for i := 0; i < len(subscriptions[subscriptionID].sentMessages); i++ {
				if subscriptions[subscriptionID].sentMessages[i] == messageID {
					indexToRemove = i
					break
				}
			}
			if indexToRemove != -1 {
				if subscriptions[subscriptionID].tocall != nil {
					subscriptions[subscriptionID].tocall(subscriptionID, messageID)
				}
			}
			
		})

}

/*

A PubSub system is a message propagation system that decouples senders and receivers.

Senders are called Publishers and Receivers are called Subscribers. These are the clients/users of the system.

There are two components in the system: Topics and Subscriptions. There is one to many mapping between Topics and Subscriptions.
Publisher publishes message to a Topic and Subscribers subscribe to Subscriptions to receive these messages. Any message sent to a Topic is propagated to all of its Subscriptions. There can be only one subscriber attached to a subscription.

Subscriptions are push based, they send the message to the subscriber instead of letting the subscriber pull.

The PubSub system should support below methods:
CreateTopic(topicID)
DeleteTopic(TopicID)
AddSubscription(topicID,SubscriptionID); Creates and adds subscription with id SubscriptionID to topicName.
DeleteSubscription(SubscriptionID)
Publish(topicID, message); publishes the message on given topic
Subscribe(SubscriptionID, SubscriberFunc); SubscriberFunc is the subscriber which is executed for each message of subscription.
UnSubscribe(SubscriptionID)
Ack(SubscriptionID, MessageID); Called by Subscriber to intimate the Subscription that the message has been received and processed.

Please handle retry in case a subscriber is not able to ack message within a time limit.

I understand that you have very little exposure to Go. Please take this a quick ramp up :D
Go is a very simple and small language with literally just 25 keywords.

Here are a few resources to get started (in recommended order):
A brief Tour: https://tour.golang.org)
Lang Spec (concise and highly rec'd) :https://golang.org/ref/spec )
Writing effective Go:https://golang.org/doc/effective_go.html

There are more resources on https://golang.org/doc/ that you may want to check out e.g this talk on concurrency patterns: https://www.youtube.com/watch?v=f6kdp27TYZs

*/
