# qadom

**experimental**

*peer-to-peer social network for question answering*

inspired from Quora and StackOverflow.

## Kesako?

(See below for the english version)

Ce projet vise à créer un réseau social pair-à-pair de
question-réponse. Un système décentralisé sans serveur principal [0].

Contrairement aux services centralises tel que Quora ou StackOverflow,
Qadom n’est pas sous le contrôle d’une (seule) entreprise, ne vous
espionne pas et ne vend pas vos données au plus offrant.

Les outils tel que Mastodon ou Pleroma basés sur les recommandations
du W3C, entre autre la norme ActivityPub, utilisent encore le
paradigme client-serveur pour créer le réseau de fédération, aussi
appelé le Fediverse. Cela veux dire que pour participer à ce(s)
réseau(x) il faut encore qu’une personne bien veillante héberge une
instance d’un de ces logiciels. Un centre vers lequel d’autres
personnes vont se tourner pour héberger leur(s) identité(s) et rendre
possible la conversation.

Qadom est un réseau participatif. Avec Qadom, n’importe qui pourra
héberger sur son ordinateur personnel une partie du réseau un peu et
participer comme avec le réseau BitTorrent.

A l'aide de Qadom, il deviens moins nécessaire l’existence de ces
centres [0], avec l’avantage de toujours garder le contrôle sur son
identité.

[0] Il existe (existera) des serveurs “facilitateurs” accessibles
	publiquement sur Internet qui mettent (metteront) en contact les
	différentes pairs. Il sera aussi possible de decouvrir des pairs
	en soundant le réseau local ou directement en entrant les adresses
	de vos amis.

Contrairement aux services basés sur la norme ActivityPub, vous garder
toujours votre identité sur votre ordinateur. Il n’y a pas de notion
d’instance tel que dans le Fediverse, il en suit qu’il n’y pas besoin
d’avoir de compte de secours et qu’il n’est pas nécessaire de migrer
votre profile d’un serveur à un autre car vos données publics sont
partagées par vous et par les autres participants. A moins que le
réseau soit rompu, pour une raison ou une autre, il y a un seul réseau
Qadom.

Qadom un compromis pour avoir de meilleurs performances et
utilisabilité par rapport aux réseaux gnunet, Freenet et Tor… Même si
la résistance aux attaques contre le réseaux pair-a-pair formés par
les participants fait partie des objectifs de Qadom. Qadom privilégie
des interactions riches et proche de l’expérience du Fediverse avec
l’avantage, comme discuté plus haut, d’avoir une plus grande liberte
et plus grand contrôle sur son identité.

Contrairement à Freenet, gnunet et Tor, pour le moment en tout cas,
Qadom va (uniquement) sécuriser les communications pair-à-pair de
façon à éviter qu’une entité malveillante puisse surveiller le réseau
sans avoir à participer, au moins dans une certaines mesure,
honnêtement au réseau . C’est une mécanique similaire au HTTPS. Une
partie tierce ne peux pas savoir ce que vous dites à votre
interlocuteur, mais votre interlocuteur est au courant de votre
intention. Il existe différente façon de protéger vos intentions à
votre interlocuteur tel faire des demandes fantaisistes (déjà utilisé
dans Qadom pour envoyer les pairs malveillantes au goulag) ou relayer
des demandes d’autres pairs (cela s’appelle “cover traffic” dans
gnunet).

Enfin, il faut remarquer que la sécurité de la vie privée dans Qadom
n’est pas pire que dans le Fediverse (si vous n’utilisez pas Tor (même
si, apparemment, Tor a des failles)). Sachant que avec Qadom vous ne
paratagerez jamais votre mot-de-passe avec une autre pair (meme
bien-veillante).

Je connais peu ou pas IPFS, Secure Scuttlebut (SSB) et DAT. Donc je ne
pourrais pas comparer avec ces réseaux. Une différence avec ses
outils, c’est que Qadom est écrit en Python et que, à priori, c’est
facile à implémenter dans d’autres langages.

## What?

This project aims to create a question-answer peer-to-peer social
network. A decentralized system without primary server [0].

Unlike centralized services Quora or StackOverflow, Qadom is not
controlled by a single entity, it does not spy on you, it doesn't sell
your profile.

Tools like Mastodon or Pleroma are based on recommendations by the
W3C, among others ActivityPub, still rely on the client-server
paradigm to create a federation, known as the Fediverse. It means that
to take part in that universe, one needs a benevolant person to host
for them an instance of those softwares. It is central hub, where
people can go, register and host their identity and make the community
happen.

Qadom is a cooperative network. With Qadom, anybody can host on their
computer part of the network and take part in it like with BitTorrent.

With the help of Qadom, there is less needs for central hubs [0]. It
also has the advantage that you own your data and identity.

[0] There will be central hubs accessible over the Internet that will
	allow peers to connect to each other. That will be optional and
	peers can connect to any other peer making it possible to build
	friend of friend kind of networks.

Unlike applications based on ActivityPub, you keep control of your
identity.  There is no concept of instance like in the Fediverse. One
does not need a backup account in case the instance they choosed goes
away.  There is no concept of migration in Qadom.

Except in case of network split, for a reason or another, there is a
single Qadom network.

Compared to gnunet, Freenet or Tor, Qadom is a trade-off for better
performance and usability. Nonetheless, being censorship resistant and
being able to cope with various attacks against the network are goals
of the project. In a first step, Qadom want to proove that it is
possible to build applications with rich interactions on a
peer-to-peer network and an experience similar to the Fediverse. And
like stated previously, with the great advantage of more freedom and
full control over one's identity.

Unlike Freenet, gnunet and Tor, for the time being at least, Qadom
will (only) sercure point-to-point communications between peers. So
that a malevolent operator will not be able to evavedrop on the whole
network without taking part in the network somewhat honestly. It is a
mechanic similar to HTTPS, only the other end of the link knows about
what one's intention. Also, there are technics to confuse the other
peers about one's intention that boils down to send mock traffic or
forwarding other peers traffic. That is called cover traffic in
gnunet.

Finally, one must recognize that security is not worse than in the
ActivityPub based applications (except if you use Tor (which,
apparantly is also flowed anyway)). And with Qadom, one never share
their password with a (benevolant?) third party.

I don't know much about IPFS, Secure Scuttlebut (SSB) and DAT. So, I
can not compare with their approach to peer-to-peer (feedback
welcome!). One difference with those projects is that Qadom is coded
in Python and that, a priori, it is easy to interop with from another
programming language.

## How?

To get started, install the dependencies with the following command:

```shell
pip install --user pipenv
git clone https://github.com/amirouche/qadom
cd qadom
pipenv shell
pipenv install --dev
```

Then in a first terminal run:

```shell
pipenv run make devrun
```

In other terminal

```shell
pipenv run make devrun2
```

Then open two browser windows:

```
xdg-open http://localhost:8000
xdg-open http://localhost:8002
```

Don't forget to refresh the page, the application doesn't use
javascript.
