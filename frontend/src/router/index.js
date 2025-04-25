import { createRouter, createWebHistory } from 'vue-router'
import Home from '../views/Home.vue'
import PetNameGameView from '../views/games/PetNameGameView.vue'
import MagicCardView from '../views/games/MagicCardView.vue'


const routes = [
  {
    path: '/',
    name: 'Home',
    component: Home
  },
  {
    path: '/games/pet-name-game',
    name: 'PetNameGame',
    component: PetNameGameView
  },
  {
    path: '/games/magic-card',
    name: 'MagicCard',
    component: MagicCardView
  },
  // Add more game routes as needed
]

const router = createRouter({
  history: createWebHistory(),
  routes
})

export default router




// const router = createRouter({
//   history: createWebHistory(),
//   routes: [
//     {
//       path: '/',
//       name: 'home',
//       component: () => import('../views/Home.vue')
//     }
//   ]
// })

// export default router;