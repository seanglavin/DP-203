import { createRouter, createWebHistory } from 'vue-router'
import Home from '../views/Home.vue'
import PetNameGameView from '../views/games/PetNameGameView.vue'


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